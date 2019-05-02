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
    std::cerr << "File size: " << size << std::endl;

    if (size * 2 <= LIMIT_IN_MEMORY_SORT) // input and output fit into memory
    {
        std::cerr << "Sort in-memory" << std::endl;
        sort_inmemory_distribute(infile, size, outfile, threadCount);
    }
    else if (size <= LIMIT_IN_MEMORY_SORT) // only input fits into memory
    {
        std::cerr << "Sort in-memory distribute" << std::endl;
        sort_inmemory_distribute(infile, size, outfile, threadCount);
    }
    else // neither input nor output fits into memory
    {
        return;
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
