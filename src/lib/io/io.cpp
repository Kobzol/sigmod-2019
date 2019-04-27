#include "io.h"
#include "mmap-writer.h"
#include "file-writer.h"
#include "../sync.h"
#include "../sort/merge.h"
#include "worker.h"

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
    fileOutput.preallocate(count);

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
    FileWriter writer(output.c_str());
    writer.preallocate(count);

    SyncQueue<IORequest> ioQueue;
    SyncQueue<size_t> notifyQueue;

    std::thread ioThread = ioWorker(ioQueue);

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
        ioQueue.push(IORequest(outBuffer.getActiveBuffer(), to_handle, outBuffer.processedCount, &notifyQueue,
                &writer));
        outBuffer.swapBuffer();
    }
    notifyQueue.pop();
    ioQueue.push(IORequest::last());
    ioThread.join();

    std::cerr << "Buffer IO: " << bufferIOWrite << std::endl;
}

void write_mmap(const Record* __restrict__ records, const uint32_t* __restrict__ sorted, ssize_t count,
        const std::string& output, size_t threads)
{
    MmapWriter writer(output.c_str(), count);
    auto* __restrict__ target = writer.get_data();

#pragma omp parallel for num_threads(threads / 2)
    for (ssize_t i = 0; i < count; i++)
    {
        target[i] = records[sorted[i]];
    }
    /*int fd = open(output.c_str(), O_RDWR | O_CREAT | O_APPEND, 0666);
    CHECK_NEG_ERROR(fd);
    CHECK_NEG_ERROR(ftruncate64(fd, count * TUPLE_SIZE));

    int chunks = 4;
    ssize_t perChunk = std::ceil(count / (double) chunks);

    ssize_t written = 0;
    for (int c = 0; c < chunks; c++)
    {
        Timer timerCopy;
        auto length = std::min(perChunk, count - written);
        auto byteLength = length * TUPLE_SIZE;
        auto offset = written * TUPLE_SIZE;
        size_t moveOffset = 0;

        if (offset % 4096 != 0)
        {
            moveOffset = offset % 4096;
            offset -= moveOffset;
            byteLength += moveOffset;
        }

        auto* __restrict__ target = static_cast<char*>(mmap64(nullptr, byteLength, PROT_WRITE, MAP_SHARED,
                                                                  fd, offset));
        CHECK_NEG_ERROR((ssize_t) target);
        auto* __restrict__ writeTarget = reinterpret_cast<Record*>(target + moveOffset);

        auto* __restrict__ start = sorted + written;

#pragma omp parallel for num_threads(threads / 2)
        for (ssize_t i = 0; i < length; i++)
        {
            writeTarget[i] = records[start[i]];
        }

        timerCopy.print("Copy");

        Timer timerUnmap;
        CHECK_NEG_ERROR(munmap(target, byteLength));
        timerUnmap.print("Unmap");

        written += length;
    }

    CHECK_NEG_ERROR(close(fd));

    auto mask = _mm_set1_epi8(0xFF);

#pragma omp parallel for num_threads(threads / 2)
    for (ssize_t i = 0; i < count; i++)
    {
        target[i] = records[sorted[i]];
        auto* ptr = reinterpret_cast<const char*>(records + sorted[i]);
        auto* dest = reinterpret_cast<char*>(&target[i]);
        _mm256_storeu_si256((__m256i*)&target[i][0], _mm256_loadu_si256((__m256i*)&(*ptr)[0]));
        _mm256_storeu_si256((__m256i*)&target[i][32], _mm256_loadu_si256((__m256i*)&(*ptr)[32]));
        _mm256_storeu_si256((__m256i*)&target[i][64], _mm256_loadu_si256((__m256i*)&(*ptr)[64]));
        *(reinterpret_cast<uint32_t*>(&target[i][96])) = *(reinterpret_cast<const uint32_t*>(&(*ptr)[96]));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest), *reinterpret_cast<const long long int*>(ptr));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 8), *reinterpret_cast<const long long int*>(ptr + 8));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 16), *reinterpret_cast<const long long int*>(ptr + 16));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 24), *reinterpret_cast<const long long int*>(ptr + 24));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 32), *reinterpret_cast<const long long int*>(ptr + 32));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 40), *reinterpret_cast<const long long int*>(ptr + 40));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 48), *reinterpret_cast<const long long int*>(ptr + 48));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 56), *reinterpret_cast<const long long int*>(ptr + 56));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 64), *reinterpret_cast<const long long int*>(ptr + 64));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 72), *reinterpret_cast<const long long int*>(ptr + 72));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 80), *reinterpret_cast<const long long int*>(ptr + 80));
        _mm_stream_si64(reinterpret_cast<long long int*>(dest + 88), *reinterpret_cast<const long long int*>(ptr + 88));
        _mm_stream_si32(reinterpret_cast<int*>(dest + 96), *reinterpret_cast<const int*>(ptr + 96));
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr)), mask, dest);
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 16)), mask, dest + 16);
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 32)), mask, dest + 32);
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 48)), mask, dest + 48);
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 64)), mask, dest + 64);
        _mm_maskmoveu_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 80)), mask, dest + 80);
        _mm_stream_si32(reinterpret_cast<int*>(dest + 96), *reinterpret_cast<const int*>(ptr + 96));
    }*/
}
