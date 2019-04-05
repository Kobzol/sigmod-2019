#include "sort.h"

#include "../timer.h"
#include "../io/mmap-reader.h"
#include "../../settings.h"
#include "../io/mmap-writer.h"
#include "../compare.h"
#include "../io/memory-reader.h"
#include "../io/file-writer.h"
#include "buffer.h"
#include "../io/io.h"
#include "merge.h"

#include <vector>
#include <queue>
#include <atomic>

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    std::vector<MergeRange> mergeRanges(SORT_GROUP_COUNT);
    size_t offset = 0;

    auto buffer = std::unique_ptr<Record[]>(new Record[EXTERNAL_SORT_PARTIAL_COUNT]);
    auto sortBuffer = std::unique_ptr<SortRecord[]>(new SortRecord[EXTERNAL_SORT_PARTIAL_COUNT]);

    while (offset < count)
    {
        Timer timerRead;
        size_t partialCount = std::min(count - offset, static_cast<size_t>(EXTERNAL_SORT_PARTIAL_COUNT));
        MemoryReader reader(infile.c_str());
        reader.read_at(buffer.get(), partialCount, offset);
        timerRead.print("Read file");

        std::string out = WRITE_LOCATION + "/out-" + std::to_string(files.size());
        std::cerr << "Writing " << partialCount << " records to " << out << std::endl;

        Timer timer;
        auto groupData = sort_records(buffer.get(), sortBuffer.get(), partialCount, threads);
        for (size_t i = 0; i < groupData.size(); i++)
        {
            mergeRanges[i].groups.push_back(groupData[i]);
        }
        timer.print("Sort file");

        Timer timerWrite;
        write_mmap(buffer.get(), sortBuffer.get(), partialCount, out, threads);
        timerWrite.print("Write");

        files.push_back(FileRecord{out, partialCount});
        offset += partialCount;
    }

    mergeRanges = remove_empty_ranges(mergeRanges);
    compute_write_offsets(mergeRanges);

    Timer timer;
    merge_files(files, mergeRanges, outfile, size, threads);
    timer.print("Merge files");
}
