#pragma once

#include <memory>
#include <atomic>

#include "../record.h"
#include "../../settings.h"
#include "../io/memory-reader.h"
#include "../io/file-writer.h"

extern std::atomic<size_t> bufferIORead;
extern std::atomic<size_t> bufferIOWrite;

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
    // how many items can be processed without reading/writing from/to file
    size_t capacity() const
    {
        return this->size - this->offset;
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
    : Buffer(bufferSize), data(std::unique_ptr<Record[]>(new Record[bufferSize])), reader(reader)
    {
        this->fileOffset = fileOffset;
        this->totalSize = totalSize;
        this->memory = this->data.get();
        this->read_from_source();
        this->loadLocal();
    }

    explicit ReadBuffer(Record* memory, size_t memorySize)
            : Buffer(memorySize), memory(memory)
    {
        this->totalSize = memorySize;
        this->processedCount = memorySize;
        this->loadLocal();
    }

    const Record& load()
    {
        return this->localCache;
    }

    void loadLocal()
    {
        this->localCache = this->memory[this->offset];
    }

    size_t read_from_source()
    {
        auto left = this->left();
        left = std::min(left, static_cast<size_t>(this->size));
        if (left)
        {
            Timer timerRead;
            this->reader->read_at(this->memory, left, this->fileOffset + this->processedCount);
            this->processedCount += left;
            this->size = left;
            this->offset = 0;
            bufferIORead += timerRead.get();
            this->loadLocal();
        }
        return left;
    }

    Record* memory = nullptr;
    std::unique_ptr<Record[]> data;
    MemoryReader* reader = nullptr;
    Record localCache;
};

struct WriteBuffer: public Buffer {
    explicit WriteBuffer(size_t size): Buffer(size)
    {
        for (auto& buffer : this->buffers)
        {
            buffer = std::unique_ptr<Record[]>(new Record[size]);
        }
    }

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
    template <bool READ_PREFETCH=true>
    bool transfer_record(ReadBuffer& other, FileWriter& writer)
    {
        this->store(other.load());
        this->offset++;
        other.offset++;

        if (EXPECT(other.needsFlush(), 0))
        {
            if (READ_PREFETCH)
            {
                return other.read_from_source() == 0;
            }
            else return true;
        }
        other.loadLocal();
        return false;
    }

    Record* getActiveBuffer()
    {
        return this->buffers[this->activeBuffer].get();
    }
    void swapBuffer()
    {
        this->activeBuffer = 1 - this->activeBuffer;
    }

    size_t activeBuffer = 0;
    std::unique_ptr<Record[]> buffers[2];
};
