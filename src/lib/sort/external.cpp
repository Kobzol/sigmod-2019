#include "sort.h"

#include "../timer.h"
#include "../io/mmap-reader.h"
#include "../../settings.h"
#include "../io/mmap-writer.h"
#include "../compare.h"
#include "../io/memory-reader.h"
#include "../io/file-writer.h"
#include "buffer.h"
#include "../io/io.h"
#include "merge.h"
#include "../sync.h"

#include <vector>
#include <queue>
#include <atomic>
#include <cmath>
#include <omp.h>

struct ReadRequest {
    Record* buffer = nullptr;
    size_t count = 0;
    size_t offset = 0;
};

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    Timer externalInit;
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    std::vector<MergeRange> mergeRanges(SORT_GROUP_COUNT);
    size_t offset = 0;

    std::vector<OverlapRange> overlapRanges;

    while (offset < count)
    {
        size_t partialCount = std::min(count - offset, static_cast<size_t>(EXTERNAL_SORT_PARTIAL_COUNT));
        OverlapRange range{ offset, offset + partialCount, 0 };
        overlapRanges.push_back(range);
        offset += partialCount;
    }

    SyncQueue<ReadRequest> readQueue;
    SyncQueue<bool> notifyQueue;

    std::thread readThread([&readQueue, &notifyQueue, &infile]() {
        MemoryReader reader(infile.c_str());

        while (true)
        {
            auto request = readQueue.pop();
            if (request.buffer == nullptr) break;
            Timer timerRead;
            reader.read_at(request.buffer, request.count, request.offset);
            timerRead.print("Read file");

            notifyQueue.push(true);
        }
    });

    std::unique_ptr<Record[]> buffers[2] = {
            std::unique_ptr<Record[]>(new Record[EXTERNAL_SORT_PARTIAL_COUNT]),
            std::unique_ptr<Record[]>(new Record[EXTERNAL_SORT_PARTIAL_COUNT])
    };
    size_t activeBuffer = 0;

    std::vector<MemoryReader> readers;

    {
        readQueue.push({ buffers[activeBuffer].get(), overlapRanges[0].count(), overlapRanges[0].start });

        auto sortBuffer = std::unique_ptr<SortRecord[]>(new SortRecord[EXTERNAL_SORT_PARTIAL_COUNT]);

        for (size_t r = 0; r < overlapRanges.size(); r++)
        {
            auto& range = overlapRanges[r];
            notifyQueue.pop();
            bool lastPart = r == overlapRanges.size() - 1;
            if (!lastPart)
            {
                // prefetch next part from disk
                readQueue.push({ buffers[1 - activeBuffer].get(), overlapRanges[r + 1].count(), overlapRanges[r + 1].start });
            }
            else
            {
                // perform readahead on the intermediate files
                for (size_t i = 0; i < readers.size(); i++)
                {
                    readers[i].readahead(files[i].count);
                }
            }

            std::string out = WRITE_LOCATION + "/out-" + std::to_string(files.size());
            std::cerr << "Writing " << range.count() << " records to " << out << std::endl;

            Timer timer;
            auto groupData = sort_records(buffers[activeBuffer].get(), sortBuffer.get(), range.count(), threads);
            for (size_t i = 0; i < groupData.size(); i++)
            {
                mergeRanges[i].groups.push_back(groupData[i]);
            }
            timer.print("Sort file");

            if (lastPart)
            {
                Timer timerPartCopy;
                auto* __restrict__ source = buffers[activeBuffer].get();
                auto* __restrict__ target = buffers[1 - activeBuffer].get();
#pragma omp parallel for num_threads(threads)
                for (size_t i = 0; i < range.count(); i++)
                {
                    target[i] = source[sortBuffer.get()[i].index];
                }
                timerPartCopy.print("Last part copy");
            }
            else
            {
                Timer timerWrite;
                write_buffered(buffers[activeBuffer].get(), sortBuffer.get(), range.count(), out,
                               WRITE_BUFFER_COUNT, threads);
                timerWrite.print("Write");

                files.push_back(FileRecord{out, range.count()});
                readers.emplace_back(out.c_str());
            }
            activeBuffer = 1 - activeBuffer;
        }
    }
    readQueue.push({nullptr, 0, 0});
    readThread.join();

    // active buffer contains the last merge part here
    // free up the second buffer
    buffers[1 - activeBuffer].reset();

    mergeRanges = remove_empty_ranges(mergeRanges);
    compute_write_offsets(mergeRanges);

    externalInit.print("External init");

    Timer timer;
    merge_files(files, readers, mergeRanges, buffers[activeBuffer].get(), overlapRanges[overlapRanges.size() - 1].count(),
            outfile, size, threads);
    timer.print("Merge files");
}
void sort_external_records(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
#define BUFFER_SIZE 100000000

    ssize_t count = size / TUPLE_SIZE;
    auto sortedOutput = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

    {
        auto sortBuffer = std::unique_ptr<SortRecord[]>(new SortRecord[count]);
        auto buffer = std::unique_ptr<Record[]>(new Record[BUFFER_SIZE]);
        MemoryReader reader(infile.c_str());

        auto chunks = std::ceil(count / (double) BUFFER_SIZE);
        auto offset = 0;

        Timer timerRead;
        for (ssize_t i = 0; i < chunks; i++)
        {
            auto length = std::min(static_cast<ssize_t>(BUFFER_SIZE), count - offset);
            if (length < 1) break;
            reader.read(buffer.get(), length);

#pragma omp parallel for num_threads(threads)
            for (ssize_t j = 0; j < length; j++)
            {
                auto& sortRecord = sortBuffer.get()[offset + j];
                sortRecord.header = get_header(buffer.get()[j]);
                sortRecord.index = offset + j;
            }
            offset += length;
        }
        timerRead.print("Read");

        Timer timerSort;
        sort_records_direct(sortBuffer.get(), sortedOutput.get(), count, threads);
        timerSort.print("Sort");
    }

    /*auto writeThreads = threads;
    ssize_t chunkSize = std::ceil(count / (double) writeThreads);
    MemoryReader reader(infile.c_str());

#pragma omp parallel num_threads(writeThreads)
    {
        FileWriter writer(outfile.c_str());
        ssize_t start = omp_get_thread_num() * chunkSize;
        auto end = std::min(count, start + chunkSize);

        writer.seek(start);
        for (ssize_t i = start; i < end; i++)
        {
            writer.splice_from(reader, sortedOutput[i].index, 1);
        }
    }*/

    MmapReader mmapReader(infile.c_str());
    FileWriter writer(outfile.c_str());

#define OUT_BUFFER_SIZE 1024 * 1024 * 10

    auto* __restrict__ source = mmapReader.get_data();
    auto buffer = std::unique_ptr<Record[]>(new Record[OUT_BUFFER_SIZE]);
    size_t bufferOffset = 0;
    auto chunks = std::ceil(count / (double) OUT_BUFFER_SIZE);

    Timer timerWrite;
    for (ssize_t i = 0; i < chunks; i++)
    {
        auto length = std::min(static_cast<size_t>(OUT_BUFFER_SIZE), count - bufferOffset);
        for (size_t j = 0; j < length; j++)
        {
            buffer.get()[j] = source[sortedOutput.get()[bufferOffset + j].index];
        }
        writer.write_at(buffer.get(), length, bufferOffset);
        bufferOffset += length;
    }
    timerWrite.print("Write");
}
