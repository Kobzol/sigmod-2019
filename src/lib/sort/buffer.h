#pragma once

#include <memory>
#include <atomic>
#include <cmath>

#include "../record.h"
#include "../../settings.h"
#include "../io/memory-reader.h"
#include "../io/file-writer.h"
#include "../memory.h"

extern std::atomic<size_t> bufferIORead;
extern std::atomic<size_t> bufferIOWrite;
extern std::atomic<size_t> mergeTime;

struct Buffer {
public:
    explicit Buffer(size_t size): size(size)
    {

    }

    // whether read buffer is empty or write buffer is full
    bool needsFlush() const
    {
        return this->offset == this->size;
    }

    // how many items need to be read/written from/to file
    size_t left() const
    {
        return this->totalSize - this->processedCount;
    }

    // read offset within the buffer
    size_t offset = 0;

    // current size of the buffer
    size_t size;

    // total number of items to read into the buffer
    size_t totalSize = 0;

    // number of items already read into the buffer
    size_t processedCount = 0;

    // offset of the file
    size_t fileOffset = 0;
};

struct ReadBuffer: public Buffer {
    explicit ReadBuffer(size_t bufferSize, size_t fileOffset, size_t totalSize, MemoryReader* reader)
    : Buffer(bufferSize), data(bufferSize), reader(reader)
    {
        this->fileOffset = fileOffset;
        this->totalSize = totalSize;
        this->memory = this->data.get();
    }

    explicit ReadBuffer(Record* memory, size_t memorySize)
            : Buffer(memorySize), memory(memory)
    {
        this->totalSize = memorySize;
        this->chunk = std::ceil(memorySize / (double) MERGE_INMEMORY_SPLIT_PARTS);
        this->processedCount = this->chunk;
        this->size = this->chunk;
    }

    const Record& load() const
    {
        return this->memory[this->offset];
    }

    size_t read_from_source(size_t size)
    {
        auto left = this->left();
        left = std::min(left, static_cast<size_t>(size));

        if (!this->reader)
        {
            auto chunk = std::max(0l, static_cast<ssize_t>(this->chunk) - 100l);
            auto address = reinterpret_cast<size_t>(this->memory + (this->processedCount - this->chunk));
            if (address % 4096 != 0)
            {
                address -= address % 4096;
            }
            CHECK_NEG_ERROR(madvise((void*) address, chunk * TUPLE_SIZE, MADV_FREE));

            if (!left) return 0;

            size_t move = std::min(this->chunk, this->totalSize - this->processedCount);
            this->processedCount += move;
            this->size += move;

            return move;
        }

        if (left)
        {
            Timer timerRead;
            this->reader->read_at(this->memory, left, this->fileOffset + this->processedCount);
            this->reader->dontneed(left, this->fileOffset + this->processedCount);
            this->processedCount += left;
            this->size = left;
            this->offset = 0;
            bufferIORead += timerRead.get();
        }
        else this->data.deallocate();

        return left;
    }

    Record* memory = nullptr;
    HugePageBuffer<Record> data;
    MemoryReader* reader = nullptr;
    size_t chunk = 0;
};

struct WriteBuffer: public Buffer {
    explicit WriteBuffer(size_t size): Buffer(size)
    {
        for (auto& buffer: this->buffers)
        {
            buffer.allocate(size);
        }
        this->activeBuffer = this->buffers[this->bufferIndex].get();
    }
    DISABLE_COPY(WriteBuffer);
    DISABLE_MOVE(WriteBuffer);

    void store(const Record& record)
    {
        this->getActiveBuffer()[this->offset] = record;
    }

    void write_to_file(FileWriter& writer)
    {
        Timer timerWrite;
        writer.write_at(this->getActiveBuffer(), this->offset, this->fileOffset + this->processedCount);
        bufferIOWrite += timerWrite.get();

        this->processedCount += this->offset;
        this->offset = 0;
    }

    // transfers item from read buffer to this write buffer
    // returns true if the read buffer is exhausted
    bool transfer_record(ReadBuffer& other, Timer& timer)
    {
        this->store(other.load());
        this->offset++;
        other.offset++;

        if (EXPECT(other.needsFlush(), 0))
        {
            mergeTime += timer.get();
            auto result = other.read_from_source(MERGE_READ_COUNT);
            timer.reset();
            return result == 0;
        }

        return false;
    }

    Record* getActiveBuffer()
    {
        return this->activeBuffer;
    }
    void swapBuffer()
    {
        this->bufferIndex = 1 - this->bufferIndex;
        this->activeBuffer = this->buffers[this->bufferIndex].get();
    }

    Record* activeBuffer;
    size_t bufferIndex = 0;
    HugePageBuffer<Record> buffers[2];
};
