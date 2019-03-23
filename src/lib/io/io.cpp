#include "io.h"
#include "mmap-writer.h"

#include <vector>
#include <memory>
#include <fstream>


void write_buffered(const Record *records, const SortRecord *sorted, size_t count, const std::string& output,
                    size_t buffer_size, size_t threads)
{
    int handle = open(output.c_str(), O_WRONLY | O_CREAT, 0666);

    auto buffer = std::unique_ptr<Record[]>(new Record[buffer_size]);

    size_t left = count;
    size_t start = 0;
    while (left > 0)
    {
        size_t to_handle = std::min(left, buffer_size);
#pragma omp parallel for num_threads(threads)
        for (size_t i = 0; i < to_handle; i++)
        {
            buffer[i] = records[sorted[start + i].index];
        }

        ssize_t written = write(handle, buffer.get(), to_handle * TUPLE_SIZE);
        CHECK_NEG_ERROR(written);
        left -= to_handle;
        start += to_handle;
    }
    close(handle);
}

void write_mmap(const Record* records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t threads)
{
    MmapWriter writer(output.c_str(), count * TUPLE_SIZE);
    auto target = writer.get_data();

#pragma omp parallel for num_threads(threads)
    for (size_t i = 0; i < count; i++)
    {
        target[i] = records[sorted[i].index];
    }
}
