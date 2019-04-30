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
#include <sys/uio.h>
#include <x86intrin.h>

void sort_inmemory(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    ssize_t count = size / TUPLE_SIZE;

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

    auto indices = std::unique_ptr<uint32_t[]>(new uint32_t[count]);
    {
        auto output = std::unique_ptr<SortRecord[]>(new SortRecord[count]);
        auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[count]);

        Timer timerSort;
        sort_records(buffer.get(), output.get(), targets.get(), count, threads);
        timerSort.print("Sort");

#pragma omp parallel for num_threads(threads)
        for (ssize_t i = 0; i < count; i++)
        {
            indices[i] = output[i].index;
        }
    }

    Timer timerWrite;
    write_mmap(buffer.get(), indices.get(), static_cast<size_t>(count), outfile, threads);
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
    {
        auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[perPart]);

        for (auto& sortedRecord: sortedRecords)
        {
            auto range = queue.pop();
            Timer timerSort;
            auto groupData = sort_records(buffer.get() + range.start, sortedRecord.get(), targets.get(), range.count(), threads);
            for (size_t i = 0; i < groupData.size(); i++)
            {
                mergeRanges[i].groups.push_back(groupData[i]);
            }
            timerSort.print("Sort");
        }
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

struct MemoryRegion {
    MemoryRegion() = default;

    DISABLE_COPY(MemoryRegion);
    DISABLE_MOVE(MemoryRegion);

    void alloc(size_t count)
    {
        this->address = static_cast<Record*>(mmap64(nullptr, count * TUPLE_SIZE, PROT_READ | PROT_WRITE,
                                    MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
        CHECK_NEG_ERROR((ssize_t) this->address);
        CHECK_NEG_ERROR(madvise(this->address, count * TUPLE_SIZE, MADV_HUGEPAGE));
        this->capacity = count;
    }
    void realloc(size_t count)
    {
        this->address = static_cast<Record*>(mremap(this->address, this->capacity * TUPLE_SIZE, count * TUPLE_SIZE,
                MREMAP_MAYMOVE));
        CHECK_NEG_ERROR((ssize_t) this->address);
        CHECK_NEG_ERROR(madvise(this->address, count * TUPLE_SIZE, MADV_HUGEPAGE));
        this->capacity = count;
    }

    bool is_full() const
    {
        return this->count == this->capacity;
    }

    void write(const Record& record)
    {
        this->address[this->count++] = record;
    }

    void dealloc()
    {
        CHECK_NEG_ERROR(munmap(this->address, this->capacity * TUPLE_SIZE));
    }

    size_t count = 0;
    size_t capacity = 0;
    Record* address = nullptr;
};

void sort_inmemory_distribute(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    Timer timerDistribute;
    ssize_t count = size / TUPLE_SIZE;

    std::vector<OverlapRange> ranges;
    auto perPart = static_cast<size_t>(std::ceil(count / (double) INMEMORY_DISTRIBUTE_OVERLAP_PARTS));
    std::unique_ptr<Record[]> readBuffers[2] = {
            std::unique_ptr<Record[]>(new Record[perPart]),
            std::unique_ptr<Record[]>(new Record[perPart])
    };
    size_t activeBuffer = 0;

    for (int i = 0; i < INMEMORY_DISTRIBUTE_OVERLAP_PARTS; i++)
    {
        auto start = i * perPart;
        auto end = std::max(start, std::min(static_cast<size_t>(count), start + perPart));
        OverlapRange range{ start, end, 0 };
        ranges.push_back(range);
    }

    SyncQueue<OverlapRange> queue;
    SyncQueue<Record*> work;

    size_t ALLOC_SIZE = 4096;
    size_t REALLOC_SIZE = count / 256;

    MemoryRegion regions[256];
    for (auto& region: regions)
    {
        region.alloc(ALLOC_SIZE);
    }

    std::thread readThread([&ranges, &work, &queue, &infile]() {
        MemoryReader reader(infile.c_str());

        for (int i = 0; i < INMEMORY_DISTRIBUTE_OVERLAP_PARTS; i++)
        {
            auto buffer = work.pop();
            if (buffer == nullptr) break;
//            Timer timerRead;
            reader.read(buffer, ranges[i].count());
//            timerRead.print("Read");
            queue.push(ranges[i]);
        }
    });

    size_t numThreads = 8;
    work.push(readBuffers[activeBuffer].get());
    for (int p = 0; p < INMEMORY_DISTRIBUTE_OVERLAP_PARTS; p++)
    {
        auto range = queue.pop();
        if (p != INMEMORY_DISTRIBUTE_OVERLAP_PARTS - 1)
        {
            work.push(readBuffers[1 - activeBuffer].get());
        }

        auto rangeCount = range.count();
        auto active = readBuffers[activeBuffer].get();

//        Timer timerInnerDistribute;
#pragma omp parallel num_threads(numThreads)
        {
            auto id = omp_get_thread_num();
            size_t perThread = std::ceil(256 / (double) numThreads);
            size_t start = id * perThread;
            size_t end = std::min(256ul, start + perThread);
            if (start < end)
            {
                for (size_t i = 0; i < rangeCount; i++)
                {
                    auto value = active[i][0];
                    if (value >= start && value < end)
                    {
                        auto& region = regions[value];
                        if (EXPECT(region.is_full(), 0))
                        {
                            region.realloc(region.capacity + (REALLOC_SIZE));
                        }
                        region.write(active[i]);
                    }
                }
            }
        }
//        timerInnerDistribute.print("Distribute inner");

        activeBuffer = 1 - activeBuffer;
    }
    readThread.join();

    for (auto& buffer: readBuffers)
    {
        buffer.reset();
    }

    timerDistribute.print("Distribute");

    Timer timerSort;
#pragma omp parallel for num_threads(10) schedule(dynamic)
    for (size_t i = 0; i < 256; i++)
    {
        if (regions[i].count)
        {
            msd_radix_sort(regions[i].address, regions[i].count);
        }
    }
    timerSort.print("Sort");

    Timer timerFinish;

    MmapWriter writer(outfile.c_str(), count);
    auto* __restrict__ target = writer.get_data();

    for (int i = 0; i < 256; i++)
    {
        ssize_t regionCount = regions[i].count;
        if (regionCount)
        {
//            Timer timerWrite;
#pragma omp parallel for num_threads(threads / 2)
            for (ssize_t j = 0; j < regionCount; j++)
            {
                target[j] = regions[i].address[j];
            }
            regions[i].dealloc();
            target += regionCount;
//            timerWrite.print("write");
        }
        else regions[i].dealloc();
    }

    timerFinish.print("Finish");
}
