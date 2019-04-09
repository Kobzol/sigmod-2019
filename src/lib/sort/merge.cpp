#include "merge.h"
#include "buffer.h"
#include "../compare.h"
#include "../io/mmap-reader.h"
#include "../sync.h"

#include <queue>
#include <atomic>
#include <algorithm>
#include <sys/sendfile.h>
#include <unordered_map>

template <typename Queue>
static void merge_loop(WriteBuffer& outBuffer, std::vector<ReadBuffer>& buffers,
        Queue& heap, FileWriter& writer, std::vector<MemoryReader>& readers, ssize_t count)
{
    for (ssize_t i = 0; i < count && heap.size() > 1; i++)
    {
        auto source = heap.top();
        heap.pop();
        auto read_exhausted = outBuffer.transfer_record<true>(buffers[source], writer, readers[source]);
        if (!read_exhausted)
        {
            heap.push(source);
        }
    }
}

struct WriteRequest {
    Record* buffer;
    size_t count;
    size_t offset;
};

std::atomic<size_t> bufferIORead{0};
std::atomic<size_t> bufferIOWrite{0};

void merge_range(std::vector<FileRecord>& files, std::vector<MemoryReader>& readers,
                        const MergeRange& range, const std::string& outfile)
{
    std::vector<ReadBuffer> buffers;
    size_t totalSize = 0;
    for (size_t i = 0; i < files.size(); i++)
    {
        auto& group = range.groups[i];
        buffers.emplace_back(MERGE_READ_BUFFER_COUNT);
        auto& buffer = buffers[buffers.size() - 1];
        buffer.fileOffset = group.start;
        buffer.totalSize = group.count;
        buffer.read_from_file(readers[i]);
        totalSize += group.count;
    }

    auto cmp = [&buffers](short lhs, short rhs) {
        return !cmp_record(buffers[lhs].load(), buffers[rhs].load());
    };

    std::priority_queue<short, std::vector<short>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < files.size(); i++)
    {
        if (buffers[i].totalSize)
        {
            heap.push(i);
        }
    }

    WriteBuffer outBuffer(MERGE_WRITE_BUFFER_COUNT);
    outBuffer.fileOffset = range.writeStart;

    SyncQueue<WriteRequest> writeQueue;
    SyncQueue<size_t> notifyQueue;
    FileWriter writer(outfile.c_str());

    std::thread writeThread([&writeQueue, &notifyQueue, &writer]() {
        while (true)
        {
            auto request = writeQueue.pop();
            if (request.buffer == nullptr) break;
            if (request.count)
            {
                Timer timerWrite;
                writer.write_at(request.buffer, request.count, request.offset);
                bufferIOWrite += timerWrite.get();
            }
            notifyQueue.push(request.count);
        }
    });

    notifyQueue.push(0);
    while (heap.size() > 1)
    {
        ssize_t leftToWrite = std::min(outBuffer.size, totalSize - outBuffer.processedCount);
        merge_loop(outBuffer, buffers, heap, writer, readers, leftToWrite);

        size_t written = notifyQueue.pop();
        outBuffer.processedCount += written;
        WriteRequest request{outBuffer.getActiveBuffer(), outBuffer.offset, outBuffer.fileOffset + outBuffer.processedCount};
        writeQueue.push(request);
        outBuffer.swapBuffer();
        outBuffer.offset = 0;
    }

    size_t written = notifyQueue.pop();
    outBuffer.processedCount += written;
    outBuffer.offset = 0;

    // empty read memory buffer
    auto source = heap.top();
    while (buffers[source].capacity())
    {
        ssize_t leftToWrite = std::min(outBuffer.size, buffers[source].capacity());
        for (ssize_t i = 0; i < leftToWrite; i++)
        {
            outBuffer.transfer_record<false>(buffers[source], writer, readers[source]);
        }
        outBuffer.write_to_file(writer);
    }

    // copy rest in kernel
    auto left = totalSize - outBuffer.processedCount;
    if (left)
    {
        off64_t inOffset = buffers[source].fileOffset + buffers[source].processedCount;
        off64_t outOffset = outBuffer.fileOffset + outBuffer.processedCount;
        writer.seek(outOffset);
        Timer timerWrite;
        writer.splice_from(readers[source], inOffset, left);
        bufferIOWrite += timerWrite.get();
    }

    writeQueue.push(WriteRequest{ nullptr, 0, 0 });
    writeThread.join();
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

    MergeRange range;
    range.writeStart = 0;
    for (auto& file: files)
    {
        range.groups.push_back(GroupData{0, static_cast<uint32_t>(file.count) });
    }
    Timer timerMerge;
    merge_range(files, readers, range, outfile);
    timerMerge.print("Merge group");

    std::cerr << "Merge read IO: " << (bufferIORead / 1000) << std::endl;
    std::cerr << "Merge write IO: " << (bufferIOWrite / 1000) << std::endl;

/*#pragma omp parallel for num_threads(6) schedule(dynamic)
    for (size_t i = 0; i < ranges.size(); i++)
    {
        Timer timerMerge;
        merge_range(files, readers, ranges[i], outfile);
        timerMerge.print("Merge group");
    }*/
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
