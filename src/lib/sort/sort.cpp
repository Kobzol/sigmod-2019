#include "sort.h"

#include "../timer.h"
#include "radix.h"
#include "../util.h"

#include <memory>
#include <cmath>
#include <cassert>
#include <omp.h>
#include <byteswap.h>

struct GroupTarget
{
    uint32_t group;
    uint32_t index;
};

std::vector<GroupData> sort_records(const Record* __restrict__ input, SortRecord* __restrict__ output,
                                    ssize_t count, size_t threads)
{
    Timer timerGroupInit;

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
    uint64_t minimum = 0;
    uint64_t maximum = std::numeric_limits<uint32_t>::max();
/*#pragma omp parallel for num_threads(threads / 4) reduction(min:minimum) reduction(max:maximum)
    for (ssize_t i = 0; i < count; i++)
    {
        auto value = input[i][0];
        minimum = std::min(minimum, value);
        maximum = std::max(maximum, value);
    }
    timerMinMax.print("Group min-max");*/

    uint64_t divisor = std::ceil(((static_cast<uint64_t>(maximum) - static_cast<uint64_t>(minimum)) + 1) / (double) GROUP_COUNT);
    const auto shift = static_cast<uint32_t>(std::ceil(std::log2(divisor)));
    std::cerr << "Minimum: " << minimum << ", maximum: " << maximum << ", shift: " << shift << std::endl;

    Timer timerGroupCount;
#pragma omp parallel num_threads(threads)
    {
        auto thread_id = static_cast<size_t>(omp_get_thread_num());
#pragma omp for
        for (ssize_t i = 0; i < count; i++)
        {
            auto groupIndex = (bswap_32(*reinterpret_cast<const uint32_t*>(&input[i]))) >> shift;
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

//        std::cerr << i << ": " << groupData[i].count << ", ";
    }

//    std::cerr << std::endl;
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
            output[targetIndex].header = get_header(input[i]);
            output[targetIndex].index = static_cast<uint32_t>(i);
        }
    }
    timerGroupDivide.print("Group divide");

    Timer timerGroupSort;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < lastGroup; i++)
    {
        auto& group = groupData[i];
        msd_radix_sort(output + group.start, group.count);
    }
    timerGroupSort.print("Group sort");

    return groupData;
}
std::vector<GroupData> sort_records_direct(
        const SortRecord* __restrict__ input,
        SortRecord* __restrict__ output,
        ssize_t count, size_t threads)
{
    Timer timerGroupInit;

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

    int divisor = std::ceil(((static_cast<int>(maximum) - static_cast<int>(minimum)) + 1) / (double) GROUP_COUNT);
    const auto shift = static_cast<uint32_t>(std::ceil(std::log2(divisor)));
    std::cerr << "Minimum: " << (int) minimum << ", maximum: " << (int) maximum << ", shift: " << shift << std::endl;

    Timer timerGroupCount;
#pragma omp parallel num_threads(threads)
    {
        auto thread_id = static_cast<size_t>(omp_get_thread_num());
#pragma omp for
        for (ssize_t i = 0; i < count; i++)
        {
            auto groupIndex = input[i].header[0] >> shift;
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
            output[targetIndex] = input[i];
        }
    }
    timerGroupDivide.print("Group divide");

    Timer timerGroupSort;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < lastGroup; i++)
    {
        auto& group = groupData[i];
        msd_radix_sort(output + group.start, group.count);
    }
    timerGroupSort.print("Group sort");

    return groupData;
}

std::vector<std::vector<SortRecord>> sort_records_per_parts(const Record* input, ssize_t count, size_t threads)
{
    Timer timerGroupInit;

    const int GROUP_COUNT = SORT_GROUP_COUNT;
    std::vector<std::vector<SortRecord>> groups(GROUP_COUNT);
    for (auto& group: groups)
    {
        group.reserve(count / GROUP_COUNT);
    }

    int divisor = std::ceil(256 / (double) GROUP_COUNT);
    const auto shift = static_cast<uint32_t>(std::ceil(std::log2(divisor)));

#pragma omp parallel num_threads(16)
    for (ssize_t i = 0; i < count; i++)
    {
        auto groupIndex = input[i][0] >> shift;
        if (groupIndex / 4 == omp_get_thread_num())
        {
            assert(groupIndex < GROUP_COUNT);
            groups[groupIndex].emplace_back(*(reinterpret_cast<const Header*>(&input[i])), i);
        }
    }

    timerGroupInit.print("Group init");

    Timer timerGroupSort;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < groups.size(); i++)
    {
        auto& group = groups[i];
        msd_radix_sort(group.data(), group.size());
    }
    timerGroupSort.print("Group sort");

    return groups;
}
