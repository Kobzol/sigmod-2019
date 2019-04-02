#include "sort.h"

#include "../timer.h"
#include "../io/io.h"
#include "radix.h"
#include "../../settings.h"

#include <memory>
#include <vector>
#include <cmath>
#include <cassert>
#include <omp.h>


struct GroupTarget
{
    uint32_t group;
    uint32_t index;
};

std::vector<GroupData> sort_inmemory(const Record* input, size_t size, const std::string& outfile, size_t threads)
{
    Timer timerGroupInit, timerSort;

    ssize_t count = size / TUPLE_SIZE;
    auto output = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

    const int GROUP_COUNT = SORT_GROUP_COUNT;
    auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[count]);
    std::vector<GroupData> groupData(GROUP_COUNT);
    std::vector<std::vector<uint32_t>> counts(static_cast<size_t>(threads));

    for (size_t i = 0; i < threads; i++)
    {
        counts[i].resize(static_cast<size_t>(GROUP_COUNT));
    }
    timerGroupInit.print("Group init");

    Timer timerMinMax;
    uint8_t minimum = 0;
    uint8_t maximum = 255;
    /*uint8_t minimum = 255;
    uint8_t maximum = 0;
#pragma omp parallel for num_threads(threads / 4) reduction(min:minimum) reduction(max:maximum)
    for (ssize_t i = 0; i < count; i++)
    {
        auto value = input[i][0];
        minimum = std::min(minimum, value);
        maximum = std::max(maximum, value);
    }
    timerMinMax.print("Group min-max");*/

//    int divisor = std::ceil(((static_cast<int>(maximum) - static_cast<int>(minimum)) + 1) / (double) GROUP_COUNT);
    int divisor = std::ceil(256 / (double) GROUP_COUNT);
    const auto shift = static_cast<uint32_t>(std::ceil(std::log2(divisor)));
    std::cerr << "Minimum: " << (int) minimum << ", maximum: " << (int) maximum << ", shift: " << shift << std::endl;

    Timer timerGroupCount;
#pragma omp parallel num_threads(threads)
    {
        auto thread_id = static_cast<size_t>(omp_get_thread_num());
#pragma omp for
        for (ssize_t i = 0; i < count; i++)
        {
            auto groupIndex = input[i][0] >> shift;
            assert(groupIndex < GROUP_COUNT);
            targets[i].group = static_cast<uint32_t>(groupIndex);
            targets[i].index = static_cast<uint32_t>(counts[thread_id][groupIndex]++);
        }
    }

    for (size_t i = 0; i < GROUP_COUNT; i++)
    {
        size_t groupCount = 0; // total number of items in this group
        for (size_t t = 0; t < threads; t++)
        {
            auto offset = counts[t][i];
            counts[t][i] = static_cast<uint32_t>(groupCount);
            groupCount += offset;
        }
        groupData[i].count = static_cast<uint32_t>(groupCount);
        uint32_t prevStart = i == 0 ? 0 : groupData[i - 1].start;
        uint32_t prevCount = i == 0 ? 0 : groupData[i - 1].count;
        groupData[i].start =  prevStart + prevCount;

        std::cerr << i << ": " << groupData[i].count << ", ";
    }

    std::cerr << std::endl;
    timerGroupCount.print("Group count");

    size_t lastGroup = GROUP_COUNT;
    while (lastGroup > 0 && groupData[lastGroup - 1].count == 0) lastGroup--;

    Timer timerGroupDivide;
#pragma omp parallel num_threads(threads)
    {
        auto thread_id = static_cast<size_t>(omp_get_thread_num());
#pragma omp for
        for (ssize_t i = 0; i < count; i++)
        {
            auto& group = groupData[targets[i].group];
            auto targetIndex = group.start + counts[thread_id][targets[i].group] + targets[i].index;
            output[targetIndex].header = *(reinterpret_cast<const Header*>(&input[i]));
            output[targetIndex].index = static_cast<uint32_t>(i);
        }
    }
    timerGroupDivide.print("Group divide");

    Timer timerGroupSort;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < lastGroup; i++)
    {
        auto& group = groupData[i];
        msd_radix_sort(output.get() + group.start, group.count);
    }
    timerGroupSort.print("Group sort");
    timerSort.print("Sort");

    Timer timerWrite;
    write_mmap(input, output.get(), static_cast<size_t>(count), outfile, threads);
//    write_buffered(input, output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
//    write_sequential_io(input, output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
    timerWrite.print("Write");

    return groupData;
}
