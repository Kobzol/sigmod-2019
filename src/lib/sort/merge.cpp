#include "merge.h"
#include "buffer.h"
#include "../compare.h"

#include <queue>

void merge_range(std::vector<FileRecord>& files, std::vector<MemoryReader>& readers,
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

void merge_files(std::vector<FileRecord>& files, const std::vector<MergeRange>& ranges,
                        const std::string& outfile, size_t size, size_t threads)
{
    FileWriter writer(outfile.c_str());
    writer.preallocate(size);

    std::vector<MemoryReader> readers;
    for (auto& file: files)
    {
        readers.emplace_back(file.name.c_str());
    }

#pragma omp parallel for num_threads(8) schedule(dynamic)
    for (size_t i = 0; i < ranges.size(); i++)
    {
        merge_range(files, readers, ranges[i], writer);
    }
}

void compute_write_offsets(std::vector<MergeRange>& ranges)
{
    size_t startOffset = 0;
    for (auto& range : ranges)
    {
        range.writeStart = startOffset;
        startOffset += range.size();
    }
}

std::vector<MergeRange> remove_empty_ranges(const std::vector<MergeRange>& ranges)
{
    std::vector<MergeRange> nonEmpty;

    for (auto& range: ranges)
    {
        if (range.size() > 0)
        {
            nonEmpty.push_back(range);
        }
    }

    return nonEmpty;
}
