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

std::atomic<size_t> bufferIORead{0};
std::atomic<size_t> bufferIOWrite{0};
std::atomic<size_t> mergeTime{0};

static uint8_t extract_heap(const std::vector<ReadBuffer>& buffers, const std::vector<uint8_t>& heap)
{
    uint8_t smallest = 0;
    auto size = static_cast<ssize_t>(heap.size());
    for (ssize_t i = 1; i < size; i++)
    {
        if (cmp_record(buffers[heap[i]].load(), buffers[heap[smallest]].load()))
        {
            smallest = i;
        }
    }

    return smallest;
}

static void merge_range(std::vector<ReadBuffer>& buffers, size_t totalSize,
        size_t writeOffset, const std::string& outfile)
{
    std::vector<uint8_t> heap;

    for (size_t i = 0; i < buffers.size(); i++)
    {
        if (buffers[i].totalSize)
        {
            heap.push_back(i);
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

    Timer timerMerge;
    notifyQueue.push(0);
    while (!heap.empty())
    {
        ssize_t leftToWrite = std::min(outBuffer.size, totalSize - outBuffer.processedCount);
        for (ssize_t i = 0; i < leftToWrite; i++)
        {
            auto sourceIndex = extract_heap(buffers, heap);
            auto& other = buffers[heap[sourceIndex]];
            outBuffer.store(other.load());
            outBuffer.offset++;
            other.offset++;

            if (EXPECT(other.needsFlush(), 0))
            {
                mergeTime += timerMerge.get();
                if (EXPECT(other.read_from_source(MERGE_READ_COUNT) == 0, 0))
                {
                    std::swap(heap[sourceIndex], heap[heap.size() - 1]);
                    heap.resize(heap.size() - 1);
                    if (heap.empty()) break;
                }
                timerMerge.reset();
            }
        }

        mergeTime += timerMerge.get();
        size_t written = notifyQueue.pop();
        timerMerge.reset();
        outBuffer.processedCount += written;
        WriteRequest request{outBuffer.getActiveBuffer(), outBuffer.offset, outBuffer.fileOffset + outBuffer.processedCount};
        writeQueue.push(request);
        outBuffer.swapBuffer();
        outBuffer.offset = 0;
    }
    notifyQueue.pop();
    writeQueue.push(WriteRequest{ nullptr, 0, 0 });
    writeThread.join();

    std::cerr << "Merge processing: " << mergeTime << std::endl;
}

void merge_files(std::vector<FileRecord>& files,
        std::vector<MemoryReader>& readers,
        std::vector<ReadBuffer>& buffers,
        const std::string& outfile, size_t size, size_t threads)
{
    FileWriter writer(outfile.c_str());
    writer.preallocate(size);

    size_t totalSize = size / TUPLE_SIZE;
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
