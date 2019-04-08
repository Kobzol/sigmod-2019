#pragma once

#include <memory>

#include "../record.h"
#include "../../settings.h"
#include "../io/memory-reader.h"
#include "../io/file-writer.h"


struct Buffer {
public:
    explicit Buffer(size_t size): size(size), data(std::unique_ptr<Record[]>(new Record[size]))
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

    std::unique_ptr<Record[]> data;
};

struct ReadBuffer: public Buffer {
    explicit ReadBuffer(size_t size): Buffer(size)
    {

    }

    const Record& load()
    {
        return this->data[this->offset];
    }

    size_t read_from_file(MemoryReader& reader)
    {
        auto left = this->left();
        left = std::min(left, static_cast<size_t>(this->size));
        if (left)
        {
            //Timer timerRead;
            reader.read_at(this->data.get(), left, this->fileOffset + this->processedCount);
            this->processedCount += left;
            this->size = left;
            this->offset = 0;
            //timerRead.print("Read buffer");
        }
        return left;
    }
};

struct WriteBuffer: public Buffer {
    explicit WriteBuffer(size_t size): Buffer(size)
    {

    }

    void store(const Record& record)
    {
        this->data[this->offset] = record;
    }

    void write_to_file(FileWriter& writer)
    {
        //Timer timerWrite;
        writer.write_at(this->data.get(), this->offset, this->fileOffset + this->processedCount);
        this->processedCount += this->offset;
        this->offset = 0;
        //timerWrite.print("Write buffer");
    }

    // transfers item from read buffer to this write buffer
    // returns true if the read buffer is exhausted
    template <bool READ_PREFETCH=true>
    bool transfer_record(ReadBuffer& other, FileWriter& writer, MemoryReader& reader)
    {
        this->store(other.load());
        this->offset++;
        other.offset++;

        if (EXPECT(other.needsFlush(), 0))
        {
            if (READ_PREFETCH)
            {
                return other.read_from_file(reader) == 0;
            }
            else return true;
        }
        return false;
    }
};
