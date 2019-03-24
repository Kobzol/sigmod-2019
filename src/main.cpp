#include <fstream>
#include <iostream>
#include <algorithm>
#include <array>
#include <vector>
#include <cstring>
#include <memory>
#include <atomic>
#include <queue>

#include <timer.h>
#include <cmath>
#include <io/mmap-reader.h>
#include <sort/radix.h>
#include <io/io.h>
#include <omp.h>
#include <io/mmap-writer.h>
#include "settings.h"


struct GroupTarget
{
    uint32_t group;
    uint32_t index;
};
struct GroupData
{
    uint32_t start;
    uint32_t count;
};

struct FileRecord {
    std::string name;
    size_t count = 0;
    size_t offset = 0;
};

static void sort_inmemory(const Record* input, size_t size, const std::string& outfile, size_t threads)
{
    Timer timerGroupInit;

    ssize_t count = size / TUPLE_SIZE;
    auto output = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

    constexpr int GROUP_COUNT = 64;
    auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[count]);
    std::vector<GroupData> groupData(GROUP_COUNT);
    std::vector<std::vector<uint32_t>> counts(static_cast<size_t>(threads));

    for (size_t i = 0; i < threads; i++)
    {
        counts[i].resize(static_cast<size_t>(GROUP_COUNT));
    }
    timerGroupInit.print("Group init");

    constexpr int divisor = 256 / GROUP_COUNT;
    const auto shift = static_cast<uint32_t >(std::ceil(std::log2(divisor)));

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
    for (size_t i = 0; i < GROUP_COUNT; i++)
    {
        auto& group = groupData[i];
        msd_radix_sort(output.get() + group.start, group.count);
    }
    timerGroupSort.print("Group sort");

    Timer timerWrite;
    write_mmap(input, output.get(), static_cast<size_t>(count), outfile, threads);
//    write_buffered(input, output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
    timerWrite.print("Write");
}

static void merge_files(std::vector<FileRecord>& files, const std::string& outfile, size_t size)
{
    ssize_t count = size / TUPLE_SIZE;

    MmapWriter writer(outfile.c_str(), size);
    std::vector<MmapReader> readers;
    std::vector<size_t> offsets(files.size());
    for (auto& out : files)
    {
        readers.emplace_back(out.name.c_str());
    }

    auto cmp = [&readers, &files](int lhs, int rhs) {
        auto lo = files[lhs].offset;
        auto ro = files[rhs].offset;
        return *(readers[lhs].get_data() + lo) > *(readers[rhs].get_data() + ro);
    };

    auto* out = writer.get_data();
    std::priority_queue<int, std::vector<int>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < readers.size(); i++)
    {
        heap.push(i);
    }

    for (ssize_t i = 0; i < count; i++)
    {
        auto source = heap.top();
        heap.pop();
        *out++ = *(readers[source].get_data() + files[source].offset++);
        if (files[source].offset < files[source].count)
        {
            heap.push(source);
        }
    }
}

static void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    size_t offset = 0;

    while (offset < count)
    {
        Timer timer;
        size_t partialCount = std::min(count - offset, static_cast<size_t>(EXTERNAL_SORT_PARTIAL_COUNT));
        MmapReader reader(infile.c_str(), offset * TUPLE_SIZE, partialCount * TUPLE_SIZE);

        std::string out = "out-" + std::to_string(files.size());
        std::cerr << "Writing " << partialCount << " records to " << out << std::endl;
        sort_inmemory(reader.get_data() + offset, partialCount * TUPLE_SIZE, out, threads);
        files.push_back(FileRecord{out, partialCount, 0});
        offset += partialCount;
        timer.print("Sort file");
    }

    Timer timer;
    merge_files(files, outfile, size);
    timer.print("Merge files");
}

static void sort(const std::string& infile, const std::string& outfile)
{
    auto threadCount = static_cast<size_t>(omp_get_max_threads());

    Timer timerLoad;
    MmapReader reader(infile.c_str());
    timerLoad.print("Read");

    auto size = reader.get_size();
    std::cerr << "File size: " << size << std::endl;

    if (size <= LIMIT_IN_MEMORY_SORT)
    {
        std::cerr << "Sort in-memory" << std::endl;
        sort_inmemory(reader.get_data(), size, outfile, threadCount);
    }
    else
    {
        std::cerr << "Sort external" << std::endl;
        sort_external(infile, size, outfile, threadCount);
    }
}

int main(int argc, char** argv)
{
    static_assert(EXTERNAL_SORT_PARTIAL_COUNT % TUPLE_SIZE == 0, "Partial sort size must be a multiple of tuple size");

    if (argc != 3)
    {
        std::cout << "USAGE: " << argv[0] << " [in-file] [outfile]" << std::endl;
        return 1;
    }

    std::ios::sync_with_stdio(false);

    sort(argv[1], argv[2]);
    return 0;
}
