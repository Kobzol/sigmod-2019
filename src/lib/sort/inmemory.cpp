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
#include "buffer.h"

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
        heap.push(i);
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

    std::unique_ptr<SortRecord[]> sortedRecords[INMEMORY_OVERLAP_PARTS];
    std::vector<OverlapRange> ranges;
    auto perPart = static_cast<size_t>(std::ceil(count / (double) INMEMORY_OVERLAP_PARTS));
    std::unique_ptr<Record[]> buffers[2] = {
            std::unique_ptr<Record[]>(new Record[perPart]),
            std::unique_ptr<Record[]>(new Record[perPart])
    };

    for (int i = 0; i < INMEMORY_OVERLAP_PARTS; i++)
    {
        auto start = i * perPart;
        auto end = std::min(static_cast<size_t>(count), start + perPart);
        OverlapRange range{ start, end, 0 };
        ranges.push_back(range);
        sortedRecords[i] = std::unique_ptr<SortRecord[]>(new SortRecord[range.count()]);
    }

    SyncQueue<std::pair<OverlapRange, ssize_t>> inQueue;
    SyncQueue<std::pair<OverlapRange, ssize_t>> outQueue;

    std::thread readThread([&inQueue, &outQueue, &buffers, &infile]() {
        MemoryReader reader(infile.c_str());

        size_t read = 0;
        while (read < INMEMORY_OVERLAP_PARTS)
        {
            auto job = inQueue.pop();

            auto range = job.first;
            auto bufferIndex = job.second;
            Timer timerRead;
            reader.read(buffers[bufferIndex].get(), range.count());
            timerRead.print("Read");
            outQueue.push({ range, bufferIndex });
            read++;
        }
    });

    size_t groupSize = 256;
    std::vector<std::vector<Record>> groups(groupSize);

    inQueue.push({ ranges[0], 0 });

    for (int i = 0; i < INMEMORY_OVERLAP_PARTS; i++)
    {
        auto job = outQueue.pop();
        auto range = job.first;
        auto bufferIndex = job.second;

        // start reading next chunk
        if (i < INMEMORY_OVERLAP_PARTS - 1)
        {
            inQueue.push({ ranges[i + 1], 1 - bufferIndex });
        }

        Timer timerDistribute;
        size_t numThreads = 20;
        auto perThread = static_cast<size_t>(std::ceil(groupSize / (double) numThreads));
        std::vector<size_t> bounds;
        bounds.push_back(0);
        for (int i = 0; i < numThreads + 1; i++)
        {
            bounds.push_back(bounds[i] + perThread);
        }

#pragma omp parallel num_threads(numThreads)
        {
            size_t id = omp_get_thread_num();
            size_t min = bounds[id];
            size_t max = bounds[id + 1];
            for (size_t r = 0; r < range.count(); r++)
            {
                auto& record = buffers[bufferIndex].get()[r];
                auto key = record[0];
                if (key >= min && key < max)
                {
                    groups[key].push_back(record);
                }
            }
        }
        timerDistribute.print("Distribute");
    }
    readThread.join();

    auto prefix = prefix_sum(groups);
    MmapWriter writer(outfile.c_str(), count);
    auto* __restrict__ target = writer.get_data();

    size_t start = 0;
    size_t end = groups.size();
    while (start < end && groups[start].empty()) start++;
    while (end > start && groups[end - 1].empty()) end--;

    Timer timerSort;
#pragma omp parallel for schedule(dynamic) num_threads(20)
    for (int i = start; i < end; i++)
    {
        msd_radix_sort_record(groups[i].data(), groups[i].size());
        std::memcpy(target + prefix[i], groups[i].data(), groups[i].size() * TUPLE_SIZE);
    }
    timerSort.print("Sort");
}
