#include "io.h"
#include "mmap-writer.h"
#include "file-writer.h"

#include <vector>
#include <memory>
#include <fstream>
#include <x86intrin.h>
#include <cmath>
#include <omp.h>


void write_buffered(const Record *records, const SortRecord *sorted, size_t count, const std::string& output,
                    size_t buffer_size, size_t threads)
{
    FileWriter fileOutput(output.c_str());
    fileOutput.preallocate(count * TUPLE_SIZE);

    const size_t outerThreads = 4;
    const size_t innerThreads = threads / outerThreads;
    const auto threadChunk = static_cast<size_t>(std::ceil((double) count / outerThreads));

#pragma omp parallel num_threads(outerThreads)
    {
        auto buffer = std::unique_ptr<Record[]>(new Record[buffer_size]);

        auto threadId = static_cast<size_t>(omp_get_thread_num());
        size_t start = threadId * threadChunk;
        size_t end = std::min(start + threadChunk, count);

        FileWriter writer(output.c_str());
        writer.seek(start * TUPLE_SIZE);

        size_t left = end - start;
        while (left > 0)
        {
            Timer timerCopy;
            size_t to_handle = std::min(left, buffer_size);
#pragma omp parallel for num_threads(innerThreads)
            for (size_t i = 0; i < to_handle; i++)
            {
                buffer[i] = records[sorted[start + i].index];
            }
            timerCopy.print("Write copy");

            Timer timerIO;
            writer.write(buffer.get(), to_handle);
            timerIO.print("Write I/O");
            left -= to_handle;
            start += to_handle;
        }
    }
}

void write_mmap(const Record* __restrict__ records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t threads)
{
    MmapWriter writer(output.c_str(), count * TUPLE_SIZE);
    auto* __restrict__ target = writer.get_data();

#pragma omp parallel for num_threads(threads / 2)
    for (size_t i = 0; i < count; i++)
    {
        auto* ptr = records + sorted[i].index;
//        target[i] = records[sorted[i].index];
        _mm256_storeu_si256((__m256i*)&target[i][0], _mm256_loadu_si256((__m256i*)&(*ptr)[0]));
        _mm256_storeu_si256((__m256i*)&target[i][32], _mm256_loadu_si256((__m256i*)&(*ptr)[32]));
        _mm256_storeu_si256((__m256i*)&target[i][64], _mm256_loadu_si256((__m256i*)&(*ptr)[64]));
        *(reinterpret_cast<uint32_t*>(&target[i][96])) = *(reinterpret_cast<const uint32_t*>(&(*ptr)[96]));
    }
}
