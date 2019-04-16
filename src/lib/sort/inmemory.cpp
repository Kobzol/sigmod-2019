#include "sort.h"

#include "../timer.h"
#include "../io/io.h"
#include "radix.h"
#include "../../settings.h"
#include "../io/memory-reader.h"
#include "../io/mmap-writer.h"
#include "../sync.h"
#include "../compare.h"
#include "merge.h"

#include <memory>
#include <vector>
#include <cmath>
#include <cassert>
#include <omp.h>

void sort_inmemory(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    ssize_t count = size / TUPLE_SIZE;
    auto output = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

    auto buffer = std::unique_ptr<Record[]>(new Record[count]);
    Timer timerLoad;
    MemoryReader reader(infile.c_str());

    size_t readThreads = 4;
    size_t perThread = std::ceil(count / readThreads);
#pragma omp parallel num_threads(readThreads)
    {
        size_t start = perThread * omp_get_thread_num();
        size_t end = std::min(static_cast<size_t>(count), start + perThread);
        reader.read_at(buffer.get() + start, end - start, start);
    }
    timerLoad.print("Read");

    Timer timerSort;
    sort_records(buffer.get(), output.get(), count, threads);
    timerSort.print("Sort");

    Timer timerWrite;
    write_mmap(buffer.get(), output.get(), static_cast<size_t>(count), outfile, threads);
//    write_buffered(buffer.get(), output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
//    write_sequential_io(buffer.get(), output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
    timerWrite.print("Write");
}

static void merge_inmemory(
        const Record* __restrict__ data,
        Record* __restrict__ target,
        const std::unique_ptr<SortRecord[]>* parts,
        const MergeRange& mergeRange,
        std::vector<OverlapRange> ranges,
        const std::string& outfile)
{
    auto cmp = [&parts, &ranges](short lhs, short rhs) {
        return !cmp_header(
                parts[lhs].get()[ranges[lhs].offset].header,
                parts[rhs].get()[ranges[rhs].offset].header
        );
    };

    target += mergeRange.writeStart;

    std::priority_queue<short, std::vector<short>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < ranges.size(); i++)
    {
        ranges[i].offset = mergeRange.groups[i].start;
        ranges[i].end = ranges[i].offset + mergeRange.groups[i].count;
    }

    for (size_t i = 0; i < ranges.size(); i++)
    {
        if (ranges[i].offset < ranges[i].end)
        {
            heap.push(i);
        }
    }

    while (heap.size() > 1)
    {
        auto source = heap.top();
        heap.pop();

        auto& range = ranges[source];
        auto offset = range.offset;
        range.offset++;

        *target++ = data[parts[source].get()[offset].index + range.start];

        if (range.offset < range.end)
        {
            heap.push(source);
        }
    }

    auto source = heap.top();
    auto& range = ranges[source];
    auto readPtr = parts[source].get();

    while (range.offset < range.end)
    {
        *target++ = data[readPtr[range.offset++].index + range.start];
    }
}

void sort_inmemory_overlapped(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    ssize_t count = size / TUPLE_SIZE;
    auto buffer = std::unique_ptr<Record[]>(new Record[count]);

    std::unique_ptr<SortRecord[]> sortedRecords[INMEMORY_OVERLAP_PARTS];
    std::vector<OverlapRange> ranges;
    auto perPart = static_cast<size_t>(std::ceil(count / (double) INMEMORY_OVERLAP_PARTS));

    for (int i = 0; i < INMEMORY_OVERLAP_PARTS; i++)
    {
        auto start = i * perPart;
        auto end = std::max(start, std::min(static_cast<size_t>(count), start + perPart));
        OverlapRange range{ start, end, 0 };
        ranges.push_back(range);
        sortedRecords[i] = std::unique_ptr<SortRecord[]>(new SortRecord[range.count()]);
    }

    SyncQueue<OverlapRange> queue;

    std::thread readThread([&ranges, &queue, &buffer, &infile]() {
        MemoryReader reader(infile.c_str());

        for (int i = 0; i < INMEMORY_OVERLAP_PARTS; i++)
        {
            auto range = ranges[i];
            Timer timerRead;
            reader.read(buffer.get() + range.start, range.count());
            timerRead.print("Read");
            queue.push(range);
        }
    });

    std::vector<MergeRange> mergeRanges(SORT_GROUP_COUNT);

    for (auto& sortedRecord : sortedRecords)
    {
        auto range = queue.pop();
        Timer timerSort;
        auto groupData = sort_records(buffer.get() + range.start, sortedRecord.get(), range.count(), threads);
        for (size_t i = 0; i < groupData.size(); i++)
        {
            mergeRanges[i].groups.push_back(groupData[i]);
        }
        timerSort.print("Sort");
    }
    readThread.join();

    mergeRanges = remove_empty_ranges(mergeRanges);
    compute_write_offsets(mergeRanges);

    Timer timerMerge;

    MmapWriter writer(outfile.c_str(), count);
    auto* target = writer.get_data();

#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (size_t i = 0; i < mergeRanges.size(); i++)
    {
        merge_inmemory(buffer.get(), target, sortedRecords, mergeRanges[i], ranges, outfile);
    }

    timerMerge.print("Merge");
}
