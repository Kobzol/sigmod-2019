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
#include "../io/worker.h"
#include "../memory.h"

#include <vector>
#include <queue>
#include <atomic>
#include <cmath>
#include <omp.h>
#include <sys/uio.h>

static std::vector<OverlapRange> createOverlapRanges(size_t count)
{
    std::vector<OverlapRange> overlapRanges;

    size_t inmemory = std::min(count, static_cast<size_t>(EXTERNAL_SORT_INMEMORY_COUNT));
    size_t disk = count - inmemory;

    size_t offset = 0;
    while (offset < disk)
    {
        size_t partialCount = std::min(disk - offset, static_cast<size_t>(EXTERNAL_SORT_PARTIAL_COUNT));
        overlapRanges.emplace_back(offset, offset + partialCount, 0, false );
        offset += partialCount;
    }

    overlapRanges.emplace_back(offset, count, 0, true );

    return overlapRanges;
}

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    Timer externalInit;
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    std::vector<OverlapRange> overlapRanges = createOverlapRanges(count);

    SyncQueue<IORequest> ioQueue;
    SyncQueue<size_t> notifyQueue;
    MemoryReader reader(infile.c_str());

    std::thread ioThread = ioWorker(ioQueue);

    auto offsetSize = std::max(EXTERNAL_SORT_PARTIAL_COUNT, EXTERNAL_SORT_INMEMORY_COUNT);
    auto bufferSize = 2 * offsetSize;
    HugePageBuffer<Record> buffer(bufferSize);
    Record* buffers[2] = {
            buffer.get(),
            buffer.get() + offsetSize
    };
    size_t activeBuffer = overlapRanges.size() % 2;

    std::vector<MemoryReader> readers;
    std::vector<ReadBuffer> readBuffers;
    readBuffers.reserve(files.size() + 1);

    {
        ioQueue.push(IORequest::read(buffers[activeBuffer], overlapRanges[0].count(), overlapRanges[0].start,
                &notifyQueue, &reader));

        auto sortBuffer = std::unique_ptr<SortRecord[]>(new SortRecord[offsetSize]);
        auto targets = std::unique_ptr<GroupTarget[]>(new GroupTarget[offsetSize]);

        for (size_t r = 0; r < overlapRanges.size(); r++)
        {
            auto& range = overlapRanges[r];
            notifyQueue.pop();
            reader.dontneed(range.count(), range.start);

            bool lastPart = r == overlapRanges.size() - 1;
            if (!lastPart)
            {
                // prefetch next part from disk
                ioQueue.push(IORequest::read(buffers[1 - activeBuffer], overlapRanges[r + 1].count(),
                        overlapRanges[r + 1].start, &notifyQueue, &reader));
            }
            else
            {
                for (size_t i = 0; i < files.size(); i++)
                {
                    readBuffers.emplace_back(
                            static_cast<size_t>(MERGE_READ_BUFFER_COUNT),
                            static_cast<size_t>(0),
                            files[i].count,
                            &readers[i]
                    );
                }
                for (auto& buffer: readBuffers)
                {
                    ioQueue.push(IORequest::read_buffer(MERGE_INITIAL_READ_COUNT, &notifyQueue, &buffer));
                }
            }

            std::string out = WRITE_LOCATION + "/out-" + std::to_string(files.size());

            Timer timer;
            sort_records(buffers[activeBuffer], sortBuffer.get(), targets.get(), range.count(), threads);
            timer.print("Sort file");

            if (range.memory)
            {
                Timer timerPartCopy;
                auto* __restrict__ source = buffers[activeBuffer];
                auto* __restrict__ target = buffers[1 - activeBuffer];
#pragma omp parallel for num_threads(threads)
                for (size_t i = 0; i < range.count(); i++)
                {
                    target[i] = source[sortBuffer.get()[i].index];
                }
                timerPartCopy.print("Last part copy");
            }
            else
            {
                std::cerr << "Writing " << range.count() << " records to " << out << std::endl;

                Timer timerWrite;
                write_sequential_io(buffers[activeBuffer], sortBuffer.get(), range.count(), out, WRITE_BUFFER_COUNT,
                        threads);
                timerWrite.print("Write");

                files.push_back(FileRecord{out, range.count()});
                readers.emplace_back(out.c_str());
            }
            activeBuffer = 1 - activeBuffer;
        }
    }
    Timer timerWait;
    ioQueue.push(IORequest::last());
    ioThread.join();
    timerWait.print("Wait for read");

    // active buffer contains the last merge part here
    // free up the second half of the buffer
    buffer.trim(EXTERNAL_SORT_INMEMORY_COUNT);

    externalInit.print("External init");

    readBuffers.emplace_back(buffers[activeBuffer], overlapRanges[overlapRanges.size() - 1].count());

    Timer timer;
    merge_files(files, readers, readBuffers, outfile, size, threads);
    timer.print("Merge files");
}
void sort_external_records(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
#define BUFFER_SIZE 50000000

    ssize_t count = size / TUPLE_SIZE;
    auto sortedIndices = std::unique_ptr<uint32_t[]>(new uint32_t[count]);
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
        auto sortedOutput = std::unique_ptr<SortRecord[]>(new SortRecord[count]);
        sort_records_direct(sortBuffer.get(), sortedOutput.get(), count, threads);
        timerSort.print("Sort");

        Timer timerCompress;
#pragma omp parallel for num_threads(threads)
        for (ssize_t i = 0; i < count; i++)
        {
            sortedIndices[i] = sortedOutput[i].index;
        }
        timerCompress.print("Compress");
    }

#define OUT_BUFFER_SIZE 1024
    FileWriter writer(outfile.c_str());
    MmapReader mmapReader(infile.c_str());
    auto* __restrict__ source = mmapReader.get_data();

    struct iovec vectors[OUT_BUFFER_SIZE];
    for (auto& vector: vectors)
    {
        vector.iov_len = 100;
    }

    ssize_t bufferOffset = 0;

    Timer timerFinal;
    while (bufferOffset < count)
    {
        auto length = std::min(static_cast<ssize_t>(OUT_BUFFER_SIZE), count - bufferOffset);

        for (ssize_t i = 0; i < length; i++)
        {
            vectors[i].iov_base = (void*) (source + sortedIndices[bufferOffset + i]);
        }

        Timer timerWrite;
//        CHECK_NEG_ERROR(writev(writer.file, vectors, length));
        timerWrite.print("Write");

        bufferOffset += length;
    }
    timerFinal.print("Final write");
}
