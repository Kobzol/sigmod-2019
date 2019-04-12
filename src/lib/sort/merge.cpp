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

struct WriteRequest {
    Record* buffer;
    size_t count;
    size_t offset;
};

std::atomic<size_t> bufferIORead{0};
std::atomic<size_t> bufferIOWrite{0};

void merge_range(std::vector<ReadBuffer>& buffers, size_t totalSize,
        size_t writeOffset, const std::string& outfile)
{
    auto cmp = [&buffers](short lhs, short rhs) {
        return !cmp_record(buffers[lhs].load(), buffers[rhs].load());
    };

    std::priority_queue<short, std::vector<short>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < buffers.size(); i++)
    {
        if (buffers[i].totalSize)
        {
            heap.push(i);
        }
    }

    WriteBuffer outBuffer(MERGE_WRITE_BUFFER_COUNT);
    outBuffer.fileOffset = writeOffset;

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
    while (!heap.empty())
    {
        ssize_t leftToWrite = std::min(outBuffer.size, totalSize - outBuffer.processedCount);
        for (ssize_t i = 0; i < leftToWrite && !heap.empty(); i++)
        {
            auto source = heap.top();
            heap.pop();
            auto read_exhausted = outBuffer.transfer_record<true>(buffers[source], writer);
            if (!read_exhausted)
            {
                heap.push(source);
            }
        }

        size_t written = notifyQueue.pop();
        outBuffer.processedCount += written;
        WriteRequest request{outBuffer.getActiveBuffer(), outBuffer.offset, outBuffer.fileOffset + outBuffer.processedCount};
        writeQueue.push(request);
        outBuffer.swapBuffer();
        outBuffer.offset = 0;
    }
    notifyQueue.pop();
    writeQueue.push(WriteRequest{ nullptr, 0, 0 });
    writeThread.join();
}

void merge_files(std::vector<FileRecord>& files,
        std::vector<MemoryReader>& readers,
        const std::vector<MergeRange>& ranges,
        Record* memoryBuffer, size_t memorySize,
        const std::string& outfile, size_t size, size_t threads)
{
    FileWriter writer(outfile.c_str());
    writer.preallocate(size);

    size_t totalSize = 0;
    std::vector<ReadBuffer> buffers;
    for (size_t i = 0; i < files.size(); i++)
    {
        buffers.emplace_back(
                static_cast<size_t>(MERGE_READ_BUFFER_COUNT),
                static_cast<size_t>(0),
                files[i].count,
                &readers[i]
        );
        totalSize += files[i].count;
    }
    buffers.emplace_back(memoryBuffer, memorySize);
    totalSize += memorySize;

    merge_range(buffers, totalSize, 0, outfile);

    std::cerr << "Merge read IO: " << bufferIORead << std::endl;
    std::cerr << "Merge write IO: " << bufferIOWrite << std::endl;
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
