#include "io.h"
#include "mmap-writer.h"
#include "file-writer.h"
#include "../sync.h"
#include "../sort/merge.h"

#include <vector>
#include <memory>
#include <fstream>
#include <x86intrin.h>
#include <cmath>
#include <omp.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>


struct SyncBuffer {
public:
    SyncBuffer() = default;
    explicit SyncBuffer(size_t size): size(size)
    {
        this->buffer = std::unique_ptr<Record[]>(new Record[size]);
    }
    SyncBuffer(SyncBuffer&& other) noexcept
    {
        this->size = other.size;
        this->offset = other.offset;
        this->buffer = std::move(other.buffer);
        this->dirty.store(other.dirty.load());
    }

    size_t size = 0;
    size_t offset = 0;
    std::unique_ptr<Record[]> buffer;
    std::atomic<bool> dirty{false};
};

void write_buffered(const Record *records, const SortRecord *sorted, size_t count, const std::string& output,
                    size_t buffer_size, size_t threads)
{
    FileWriter fileOutput(output.c_str());
    fileOutput.preallocate(count * TUPLE_SIZE);

    const size_t outerThreads = 4;
    const size_t innerThreads = threads / outerThreads;
    const auto threadChunk = static_cast<size_t>(std::ceil((double) count / outerThreads));

    FileWriter writer(output.c_str());

#pragma omp parallel num_threads(outerThreads)
    {
        auto buffer = std::unique_ptr<Record[]>(new Record[buffer_size]);

        auto threadId = static_cast<size_t>(omp_get_thread_num());
        size_t start = threadId * threadChunk;
        size_t end = std::min(start + threadChunk, count);

        while (start < end)
        {
            size_t left = end - start;
//            Timer timerCopy;
            size_t to_handle = std::min(left, buffer_size);
#pragma omp parallel for num_threads(innerThreads)
            for (size_t i = 0; i < to_handle; i++)
            {
                buffer[i] = records[sorted[start + i].index];
            }
//            timerCopy.print("Write copy");

//            Timer timerIO;
            writer.write_at(buffer.get(), to_handle, start);
//            timerIO.print("Write I/O");
            start += to_handle;
        }
    }
}

void write_sequential_io(const Record *records, const SortRecord *sorted, size_t count, const std::string& output,
                    size_t buffer_size, size_t threads)
{
    FileWriter fileOutput(output.c_str());
    fileOutput.preallocate(count * TUPLE_SIZE);

    SyncQueue<WriteRequest> writeQueue;
    SyncQueue<size_t> notifyQueue;
    FileWriter writer(output.c_str());

    std::thread writeThread([&writeQueue, &notifyQueue, &writer]() {
        while (true)
        {
            auto request = writeQueue.pop();
            if (request.buffer == nullptr) break;
            if (request.count)
            {
                Timer timerWrite;
                writer.write_at(request.buffer, request.count, request.offset);
                bufferIOWrite += timerWrite.get();
            }
            notifyQueue.push(request.count);
        }
    });

    WriteBuffer outBuffer(buffer_size);
    notifyQueue.push(0);

    size_t processed = 0;
    while (processed < count)
    {
        auto to_handle = std::min(buffer_size, count - processed);
        auto* buffer = outBuffer.getActiveBuffer();

        Timer timerCopy;
#pragma omp parallel for num_threads(10)
        for (size_t i = 0; i < to_handle; i++)
        {
            buffer[i] = records[sorted[processed + i].index];
        }
        processed += to_handle;
        timerCopy.print("Sequential copy");

        size_t written = notifyQueue.pop();
        outBuffer.processedCount += written;
        WriteRequest request{ outBuffer.getActiveBuffer(), to_handle, outBuffer.processedCount };
        writeQueue.push(request);
        outBuffer.swapBuffer();
    }
    notifyQueue.pop();
    writeQueue.push(WriteRequest{ nullptr, 0, 0 });
    writeThread.join();

    std::cerr << "Buffer IO: " << bufferIOWrite << std::endl;
}

void write_mmap(const Record* __restrict__ records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t threads)
{
    MmapWriter writer(output.c_str(), count);
    auto* __restrict__ target = writer.get_data();

#pragma omp parallel for num_threads(threads)
    for (size_t i = 0; i < count; i++)
    {
        target[i] = records[sorted[i].index];
        /*auto* ptr = records + sorted[i].index;
        _mm256_storeu_si256((__m256i*)&target[i][0], _mm256_loadu_si256((__m256i*)&(*ptr)[0]));
        _mm256_storeu_si256((__m256i*)&target[i][32], _mm256_loadu_si256((__m256i*)&(*ptr)[32]));
        _mm256_storeu_si256((__m256i*)&target[i][64], _mm256_loadu_si256((__m256i*)&(*ptr)[64]));
        *(reinterpret_cast<uint32_t*>(&target[i][96])) = *(reinterpret_cast<const uint32_t*>(&(*ptr)[96]));*/
    }
}
