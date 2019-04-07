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
#include <cmath>

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
    size_t count = size / TUPLE_SIZE;
    std::vector<FileRecord> files;
    std::vector<MergeRange> mergeRanges(SORT_GROUP_COUNT);
    size_t offset = 0;

    auto buffer = std::unique_ptr<Record[]>(new Record[EXTERNAL_SORT_PARTIAL_COUNT]);

    {
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

            offset += partialCount;
            Timer timerWrite;
            write_mmap(buffer.get(), sortBuffer.get(), partialCount, out, threads);
            timerWrite.print("Write");

            files.push_back(FileRecord{out, partialCount});
        }
    }

    mergeRanges = remove_empty_ranges(mergeRanges);
    compute_write_offsets(mergeRanges);

    Timer timer;
    merge_files(files, mergeRanges, outfile, size, threads);
    timer.print("Merge files");
}
void sort_external_records(const std::string& infile, size_t size, const std::string& outfile, size_t threads)
{
#define BUFFER_SIZE 100000000

    ssize_t count = size / TUPLE_SIZE;
    auto sortedOutput = std::unique_ptr<SortRecord[]>(new SortRecord[count]);

    {
        auto sortBuffer = std::unique_ptr<SortRecord[]>(new SortRecord[count]);
        auto buffer = std::unique_ptr<Record[]>(new Record[BUFFER_SIZE]);
        MemoryReader reader(infile.c_str());

        auto chunks = std::ceil(count / (double) BUFFER_SIZE);
        auto offset = 0;

        Timer timerRead;
        for (ssize_t i = 0; i < chunks; i++)
        {
            auto length = std::min(static_cast<ssize_t>(BUFFER_SIZE), count - offset);
            if (length < 1) break;
            reader.read(buffer.get(), length);

#pragma omp parallel for num_threads(threads)
            for (ssize_t j = 0; j < length; j++)
            {
                auto& sortRecord = sortBuffer.get()[offset + j];
                sortRecord.header = get_header(buffer.get()[j]);
                sortRecord.index = offset + j;
            }
            offset += length;
        }
        timerRead.print("Read");

        Timer timerSort;
        sort_records_direct(sortBuffer.get(), sortedOutput.get(), count, threads);
        timerSort.print("Sort");
    }

    MmapReader mmapReader(infile.c_str());
    FileWriter writer(outfile.c_str());

#define OUT_BUFFER_SIZE 32768

    auto* __restrict__ source = mmapReader.get_data();
    auto buffer = std::unique_ptr<Record[]>(new Record[OUT_BUFFER_SIZE]);
    size_t bufferOffset = 0;
    auto chunks = std::ceil(count / (double) OUT_BUFFER_SIZE);

    Timer timerWrite;
    for (ssize_t i = 0; i < chunks; i++)
    {
        auto length = std::min(static_cast<size_t>(OUT_BUFFER_SIZE), count - bufferOffset);
        for (size_t j = 0; j < length; j++)
        {
            buffer.get()[j] = source[sortedOutput.get()[bufferOffset + j].index];
        }
        writer.write_at(buffer.get(), length, bufferOffset);
        bufferOffset += length;
    }
    timerWrite.print("Write");
}
