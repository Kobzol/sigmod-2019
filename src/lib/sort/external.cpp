#include "sort.h"
#include "../timer.h"
#include "../io/mmap-reader.h"
#include "../../settings.h"
#include "../io/mmap-writer.h"
#include "../compare.h"

#include <vector>
#include <queue>

struct FileRecord {
    std::string name;
    size_t count = 0;
    size_t offset = 0;
};

template <typename Reader>
static void merge_files(std::vector<FileRecord>& files, std::vector<Reader>& readers,
                        const std::string& outfile, size_t size)
{
    ssize_t count = size / TUPLE_SIZE;

    MmapWriter writer(outfile.c_str(), size);
    std::vector<size_t> offsets(files.size());

    auto cmp = [&readers, &files](int lhs, int rhs) {
        auto lo = files[lhs].offset;
        auto ro = files[rhs].offset;
        return !cmp_record(*(readers[lhs].get_data() + lo), *(readers[rhs].get_data() + ro));
    };

    auto* out = writer.get_data();
    std::priority_queue<int, std::vector<int>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < readers.size(); i++)
    {
        heap.push(i);
    }

    for (ssize_t i = 0; i < count; i++)
    {
        auto source = heap.top();
        heap.pop();
        *out++ = *(readers[source].get_data() + files[source].offset++);
        if (files[source].offset < files[source].count)
        {
            heap.push(source);
        }
    }
}

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    size_t offset = 0;

    while (offset < count)
    {
        Timer timer;
        size_t partialCount = std::min(count - offset, static_cast<size_t>(EXTERNAL_SORT_PARTIAL_COUNT));
        MmapReader reader(infile.c_str(), offset * TUPLE_SIZE, partialCount * TUPLE_SIZE);

        std::string out = WRITE_LOCATION + "/out-" + std::to_string(files.size());
        std::cerr << "Writing " << partialCount << " records to " << out << std::endl;
        sort_inmemory(reader.get_data(), partialCount * TUPLE_SIZE, out, threads);
        files.push_back(FileRecord{out, partialCount, 0});
        offset += partialCount;
        timer.print("Sort file");
    }

    std::vector<MmapReader> readers;
    for (auto& out : files)
    {
        readers.emplace_back(out.name.c_str());
    }

    Timer timer;
    merge_files<MmapReader>(files, readers, outfile, size);
    timer.print("Merge files");
}
