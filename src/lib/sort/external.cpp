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

#include <vector>
#include <queue>
#include <atomic>

struct FileRecord {
    std::string name;
    size_t count = 0;
};

static void merge_range(std::vector<FileRecord>& files, std::vector<MemoryReader>& readers,
        const MergeRange& range, FileWriter& writer)
{
    std::vector<Buffer> buffers;
    for (size_t i = 0; i < files.size(); i++)
    {
        auto& group = range.groups[i];
        buffers.emplace_back(MERGE_READ_BUFFER_COUNT);
        auto& buffer = buffers[buffers.size() - 1];
        buffer.fileOffset = group.start;
        buffer.totalSize = group.count;
        buffer.read_buffer(readers[i], buffer);
    }

    auto cmp = [&buffers](short lhs, short rhs) {
        return !cmp_record(buffers[lhs].read(), buffers[rhs].read());
    };

    std::priority_queue<short, std::vector<short>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < files.size(); i++)
    {
        if (buffers[i].totalSize)
        {
            heap.push(i);
        }
    }

    Buffer outBuffer(MERGE_WRITE_BUFFER_COUNT);
    outBuffer.fileOffset = range.writeStart;

    while (heap.size() > 1)
    {
        auto source = heap.top();
        heap.pop();
        if (!outBuffer.transfer_record(buffers[source], writer, readers[source]))
        {
            heap.push(source);
        }
    }

    auto source = heap.top();
    while (!outBuffer.transfer_record(buffers[source], writer, readers[source]));
    if (outBuffer.offset)
    {
        outBuffer.write_buffer(writer, outBuffer);
    }
}

static void merge_files(std::vector<FileRecord>& files, const std::vector<MergeRange>& ranges,
        const std::string& outfile, size_t size, size_t threads)
{
    FileWriter writer(outfile.c_str());
    writer.preallocate(size);

    std::vector<MemoryReader> readers;
    for (auto& file: files)
    {
        readers.emplace_back(file.name.c_str());
    }

    int start = 0;
    int end = ranges.size();

    while (start < end && ranges[start].size() == 0) start++;
    while (end > start && ranges[end - 1].size() == 0) end--;

    if (start < end)
    {
#pragma omp parallel for num_threads(8) schedule(dynamic)
        for (int i = start; i < end; i++)
        {
            merge_range(files, readers, ranges[i], writer);
        }
    }
}

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
        reader.read(buffer.get(), partialCount, offset);
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

    size_t startOffset = 0;
    for (auto& range : mergeRanges)
    {
        range.writeStart = startOffset;
        startOffset += range.size();
    }

    Timer timer;
    merge_files(files, mergeRanges, outfile, size, threads);
    timer.print("Merge files");
}
