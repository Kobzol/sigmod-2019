#include "sort.h"

#include "../timer.h"
#include "../io/io.h"
#include "radix.h"
#include "../../settings.h"
#include "../io/memory-reader.h"
#include "../io/mmap-writer.h"

#include <memory>
#include <vector>
#include <cmath>
#include <cassert>
#include <omp.h>

void sort_inmemory(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    ssize_t count = size / TUPLE_SIZE;
    auto output = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

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

    Timer timerSort;
    sort_records(buffer.get(), output.get(), count, threads);
    timerSort.print("Sort");

    Timer timerWrite;
    write_mmap(buffer.get(), output.get(), static_cast<size_t>(count), outfile, threads);
//    write_buffered(buffer.get(), output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
//    write_sequential_io(buffer.get(), output.get(), static_cast<size_t>(count), outfile, WRITE_BUFFER_COUNT, threads);
    timerWrite.print("Write");
}
