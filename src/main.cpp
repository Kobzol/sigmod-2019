#include <iostream>
#include <omp.h>

#include <timer.h>
#include <io/mmap-reader.h>
#include <io/memory-reader.h>
#include <sort/sort.h>
#include <vector>
#include "settings.h"

static void sort(const std::string& infile, const std::string& outfile)
{
    auto threadCount = static_cast<size_t>(omp_get_max_threads());

    MemoryReader reader(infile.c_str());

    auto size = reader.get_size();
    auto count = size / TUPLE_SIZE;
    std::cerr << "File size: " << size << std::endl;

    if (false)//size <= LIMIT_IN_MEMORY_SORT)
    {
        Timer timerLoad;
        reader.read(buffer.get(), count);
        timerLoad.print("Read");

        std::cerr << "Sort in-memory" << std::endl;
        sort_inmemory(buffer.get(), size, outfile, threadCount);
    }
    else
    {
        std::cerr << "Sort external" << std::endl;
        sort_external(infile, size, outfile, threadCount);
    }
}

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cout << "USAGE: " << argv[0] << " [in-file] [outfile]" << std::endl;
        return 1;
    }

    std::ios::sync_with_stdio(false);

    sort(argv[1], argv[2]);
    return 0;
}
