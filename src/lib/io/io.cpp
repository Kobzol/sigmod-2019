#include "io.h"
#include "mmap-writer.h"

#include <vector>
#include <memory>
#include <fstream>
#include <x86intrin.h>


void write_buffered(const Record *records, const SortRecord *sorted, size_t count, const std::string& output,
                    size_t buffer_size, size_t threads)
{
    int handle = open(output.c_str(), O_WRONLY | O_CREAT, 0666);
    ftruncate64(handle, count * TUPLE_SIZE);

    auto buffer = std::unique_ptr<Record[]>(new Record[buffer_size]);

    size_t left = count;
    size_t start = 0;
    while (left > 0)
    {
        Timer timerCopy;
        size_t to_handle = std::min(left, buffer_size);
#pragma omp parallel for num_threads(threads)
        for (size_t i = 0; i < to_handle; i++)
        {
            buffer[i] = records[sorted[start + i].index];
        }
        timerCopy.print("Write copy");

        Timer timerIO;
        ssize_t written = write(handle, buffer.get(), to_handle * TUPLE_SIZE);
        CHECK_NEG_ERROR(written);
        timerIO.print("Write I/O");
        left -= to_handle;
        start += to_handle;
    }
    close(handle);
}

void write_mmap(const Record* __restrict__ records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t threads)
{
    MmapWriter writer(output.c_str(), count * TUPLE_SIZE);
    auto* __restrict__ target = writer.get_data();

#pragma omp parallel for num_threads(threads)
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
