#include "io.h"
#include "mmap-writer.h"
#include "file-writer.h"
#include "worker.h"
#include "../sync.h"
#include "../sort/merge.h"
#include "../sort/buffer.h"

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

//        Timer timerCopy;
#pragma omp parallel for num_threads(10)
        for (size_t i = 0; i < to_handle; i++)
        {
            buffer[i] = records[sorted[processed + i].index];
        }
        processed += to_handle;
//        timerCopy.print("Sequential copy");

        size_t written = notifyQueue.pop();
        outBuffer.processedCount += written;
        ioQueue.push(IORequest::write(outBuffer.getActiveBuffer(), to_handle, outBuffer.processedCount, &notifyQueue,
                &writer));
        outBuffer.swapBuffer();
    }
    notifyQueue.pop();
    ioQueue.push(IORequest::last());
    ioThread.join();
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

    /*auto mask = _mm_set1_epi8(0xFF);

#pragma omp parallel for num_threads(threads / 2)
    for (ssize_t i = 0; i < count; i++)
    {
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
