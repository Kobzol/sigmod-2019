#include <fstream>
#include <iostream>
#include <algorithm>
#include <array>
#include <vector>
#include <cstring>
#include <memory>

#include <timer.h>
#include <cmath>
#include <io/mmap-reader.h>
#include <io/memory-reader.h>
#include <io/mmap-writer.h>
#include <sort/radix.h>
#include <io/file-writer.h>
#include <io/io.h>

#include "thirdparty/kxsort.h"

struct RadixTraitsRowSortRecord
{
    static const int nBytes = KEY_SIZE;

    int kth_byte(const SortRecord &x, int k) {
        return x.header[KEY_SIZE - 1 - k] & ((unsigned char) 0xFF);
    }
    bool compare(const SortRecord& lhs, const SortRecord& rhs) {
        for (int i = 0; i < KEY_SIZE; i++) {
            if (lhs.header[i] == rhs.header[i]) {
                continue;
            }
            return lhs.header[i] < rhs.header[i];
        }

        return false;
    }
};

void create_sorted_records(const Record* input, SortRecord* output, size_t count)
{
#pragma omp parallel for
    for (size_t i = 0; i < count; i++)
    {
        output[i].header = *(reinterpret_cast<const Header*>(&input[i]));
        output[i].index = static_cast<uint32_t>(i);
    }
}

void sort(const std::string& infile, const std::string& outfile)
{
    Timer timerload;
    MmapReader reader(infile.c_str());
    timerload.print("read");

    auto size = reader.get_size();
    std::cerr << "File size: " << size << std::endl;

    Timer timergroup;
    size_t num_tuples = size / TUPLE_SIZE;
    std::vector<SortRecord> sort_buffer((unsigned long) num_tuples);

    auto buffer = reader.get_data();
    create_sorted_records(buffer, sort_buffer.data(), num_tuples);

    timergroup.print("group");

    Timer timer;
    kx::radix_sort(sort_buffer.begin(), sort_buffer.end(), RadixTraitsRowSortRecord());
//    lsd_radix_sort(sort_buffer.data(), sort_buffer.data() + sort_buffer.size());
    timer.print("sort");

    Timer timerwrite;
    write_mmap(buffer, sort_buffer.data(), num_tuples, outfile);
//    write_buffered(buffer, sort_buffer.data(), num_tuples, outfile, 10 * 1024 * 1024);
    timerwrite.print("write");
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
