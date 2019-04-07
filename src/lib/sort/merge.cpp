#include "merge.h"
#include "buffer.h"
#include "../compare.h"
#include "../io/mmap-reader.h"

#include <queue>
#include <atomic>
#include <algorithm>
#include <sys/sendfile.h>
#include <unordered_map>

void merge_range(std::vector<FileRecord>& files, std::vector<MemoryReader>& readers,
                        const MergeRange& range, const std::string& outfile)
{
    std::vector<Buffer> buffers;
    size_t totalSize = 0;
    for (size_t i = 0; i < files.size(); i++)
    {
        auto& group = range.groups[i];
        buffers.emplace_back(MERGE_READ_BUFFER_COUNT);
        auto& buffer = buffers[buffers.size() - 1];
        buffer.fileOffset = group.start;
        buffer.totalSize = group.count;
        buffer.read_buffer(readers[i], buffer);
        totalSize += group.count;
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

    FileWriter writer(outfile.c_str());
    // merge until there is more than one read buffer
    while (heap.size() > 1)
    {
        auto source = heap.top();
        heap.pop();
        if (!outBuffer.transfer_record(buffers[source], writer, readers[source]))
        {
            heap.push(source);
        }
    }

    // empty memory buffers
    auto source = heap.top();
    while (!outBuffer.transfer_record_no_read(buffers[source], writer, readers[source]));
    if (outBuffer.offset)
    {
        outBuffer.write_buffer(writer, outBuffer);
    }

    // copy rest in kernel
    auto left = totalSize - outBuffer.processedCount;
    off64_t inOffset = buffers[source].fileOffset;
    off64_t outOffset = outBuffer.fileOffset + outBuffer.processedCount;
    writer.seek(outOffset);
    writer.splice_from(readers[source], inOffset, left);
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

    auto ranges2 = ranges;
    std::sort(ranges2.begin(), ranges2.end(), [](MergeRange& lhs, MergeRange& rhs){
        return lhs.size() > rhs.size();
    });

    for (int i = 0; i < 10; i++)
    {
        std::cerr << ranges2[i].size() << ": ";
        for (auto& group: ranges2[i].groups)
        {
            std::cerr << group.count << ", ";
        }
        std::cerr << std::endl;
    }

    std::atomic<size_t> total{0};
#pragma omp parallel for num_threads(20) schedule(dynamic)
    for (size_t i = 0; i < ranges.size(); i++)
    {
        Timer timerMerge;
        merge_range(files, readers, ranges[i], outfile);
        total++;
        //std::cerr << "Range " << i << ": " << ranges[i].size() << ", took " << timerMerge.get() << ", total: " << total << std::endl;
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
