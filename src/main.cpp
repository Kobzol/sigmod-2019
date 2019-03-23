#include <fstream>
#include <iostream>
#include <algorithm>
#include <array>
#include <vector>
#include <cstring>
#include <memory>
#include <atomic>

#include <timer.h>
#include <cmath>
#include <io/mmap-reader.h>
#include <sort/radix.h>
#include <io/io.h>
#include <omp.h>

struct GroupTarget
{
    uint32_t group;
    uint32_t index;
};
struct GroupData
{
    uint32_t start;
    std::atomic<uint32_t> count{0};
};

void create_sorted_records(const Record* input, SortRecord* output, size_t count, size_t threads)
{
    constexpr int GROUP_COUNT = 64;
    auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[count]);
    std::vector<GroupData> groupData(GROUP_COUNT);

    constexpr int divisor = 256 / GROUP_COUNT;
    const auto shift = static_cast<uint32_t >(std::ceil(std::log2(divisor)));

    Timer timerGroupCount;
#pragma omp parallel for num_threads(threads)
    for (size_t i = 0; i < count; i++)
    {
        auto groupIndex = input[i][0] >> shift;
        assert(groupIndex < GROUP_COUNT);
        targets[i].group = static_cast<uint32_t>(groupIndex);
        targets[i].index = static_cast<uint32_t>(groupData[groupIndex].count.fetch_add(
                1, std::memory_order::memory_order_relaxed));
    }

    for (int i = 1; i < GROUP_COUNT; i++)
    {
        std::cerr << i << ": " << groupData[i].count << ", ";
        groupData[i].start =
                groupData[i - 1].start + groupData[i - 1].count.load(std::memory_order::memory_order_relaxed);
    }
    std::cerr << std::endl;
    timerGroupCount.print("Group count");

    Timer timerGroupDivide;
#pragma omp parallel for num_threads(threads)
    for (size_t i = 0; i < count; i++)
    {
        auto& group = groupData[targets[i].group];
        auto targetIndex = group.start + targets[i].index;
        output[targetIndex].header = *(reinterpret_cast<const Header*>(&input[i]));
        output[targetIndex].index = static_cast<uint32_t>(i);
    }
    timerGroupDivide.print("Group divide");

    Timer timerGroupSort;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < GROUP_COUNT; i++)
    {
        auto& group = groupData[i];
        msd_radix_sort(output + group.start, group.count);
    }
    timerGroupSort.print("Group sort");
}

void sort(const std::string& infile, const std::string& outfile)
{
    auto threadCount = static_cast<size_t>(omp_get_max_threads());

    Timer timerLoad;
    MmapReader reader(infile.c_str());
    timerLoad.print("Read");

    auto size = reader.get_size();
    std::cerr << "File size: " << size << std::endl;

    Timer timerGroup;
    size_t num_tuples = size / TUPLE_SIZE;
    std::vector<SortRecord> sort_buffer((unsigned long) num_tuples);

    auto buffer = reader.get_data();
    create_sorted_records(buffer, sort_buffer.data(), num_tuples, threadCount);

    timerGroup.print("Sort");

    Timer timerWrite;
    write_mmap(buffer, sort_buffer.data(), num_tuples, outfile, threadCount);
//    write_buffered(buffer, sort_buffer.data(), num_tuples, outfile, 2 * 10 * 1024 * 1024ull, threadCount);
    timerWrite.print("Write");
}

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cout << "USAGE: " << argv[0] << " [in-file] [outfile]" << std::endl;
        return 1;
    }

    std::ios::sync_with_stdio(false);

    sort(argv[1], argv[2]);
    return 0;
}
