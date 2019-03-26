#include <iostream>
#include <omp.h>

#include <timer.h>
#include <io/mmap-reader.h>
#include <sort/sort.h>
#include <vector>
#include "settings.h"

static void sort(const std::string& infile, const std::string& outfile)
{
    auto threadCount = static_cast<size_t>(omp_get_max_threads());

    Timer timerLoad;
    MmapReader reader(infile.c_str());
    timerLoad.print("Read");

    auto size = reader.get_size();
    std::cerr << "File size: " << size << std::endl;

    if (size <= LIMIT_IN_MEMORY_SORT)
    {
        std::cerr << "Sort in-memory" << std::endl;
        sort_inmemory(reader.get_data(), size, outfile, threadCount);
    }
    else
    {
        std::cerr << "Sort external" << std::endl;
        sort_external(infile, size, outfile, threadCount);
    }
}

int main(int argc, char** argv)
{
    static_assert(EXTERNAL_SORT_PARTIAL_COUNT % TUPLE_SIZE == 0, "Partial sort size must be a multiple of tuple size");

    if (argc != 3)
    {
        std::cout << "USAGE: " << argv[0] << " [in-file] [outfile]" << std::endl;
        return 1;
    }

    std::ios::sync_with_stdio(false);

    sort(argv[1], argv[2]);
    return 0;
}
