#include "io.h"
#include "mmap-writer.h"
#include "file-writer.h"
#include "../sync.h"

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

    SyncQueue<SyncBuffer*> queue;
    std::thread ioThread([&queue, &fileOutput]() {
        while (true)
        {
            auto item = queue.pop();
            if (item == nullptr) break;
            Timer timerWrite;
            fileOutput.write(item->buffer.get(), item->offset);
            item->dirty.store(false);
            timerWrite.print("Sequential write");
        }
    });

    std::vector<SyncBuffer> buffers;
    for (int i = 0; i < 2; i++)
    {
        buffers.emplace_back(buffer_size);
    }
    int activeBuffer = 0;

    size_t copied = 0;
    while (copied < count)
    {
        auto& buffer = buffers[activeBuffer];

        while (buffer.dirty)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        auto to_handle = std::min(buffer.size, count - copied);
        buffer.offset = to_handle;

        Timer timerCopy;
#pragma omp parallel for num_threads(threads / 2)
        for (size_t i = 0; i < to_handle; i++)
        {
            buffer.buffer[i] = records[sorted[copied + i].index];
        }
        copied += to_handle;
        timerCopy.print("Sequential copy");

        buffer.dirty.store(true);
        queue.push(&buffer);

        activeBuffer = 1 - activeBuffer;
    }

    for (auto& buffer: buffers)
    {
        while (buffer.dirty)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    queue.push(nullptr);
    ioThread.join();
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
