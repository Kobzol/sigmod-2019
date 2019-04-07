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

    const Record& read()
    {
        return this->data[this->offset];
    }
    void write(const Record& record)
    {
        this->data[this->offset] = record;
    }

    bool needsFlush() const
    {
        return this->offset == this->size;
    }
    size_t left() const
    {
        return this->totalSize - this->processedCount;
    }
    size_t end() const
    {
        return this->needsFlush() && this->left() == 0;
    }

    bool transfer_record(Buffer& other, FileWriter& writer, MemoryReader& reader)
    {
        this->write(other.read());
        this->offset++;
        other.offset++;

        if (EXPECT(this->needsFlush(), 0))
        {
            this->write_buffer(writer, *this);
        }
        if (EXPECT(other.needsFlush(), 0))
        {
            return this->read_buffer(reader, other) == 0;
        }
        return false;
    }
    bool transfer_record_no_read(Buffer& other, FileWriter& writer, MemoryReader& reader)
    {
        this->write(other.read());
        this->offset++;
        other.offset++;

        if (EXPECT(this->needsFlush(), 0))
        {
            this->write_buffer(writer, *this);
        }
        return other.needsFlush();
    }

    size_t read_buffer(MemoryReader& reader, Buffer& buffer)
    {
        auto left = buffer.left();
        left = std::min(left, static_cast<size_t>(MERGE_READ_BUFFER_COUNT));
        if (left)
        {
            //Timer timerRead;
            reader.read_at(buffer.data.get(), left, buffer.fileOffset);
            buffer.fileOffset += left;
            buffer.processedCount += left;
            buffer.size = left;
            buffer.offset = 0;
            //timerRead.print("Read buffer");
        }
        return left;
    }

    void write_buffer(FileWriter& writer, Buffer& buffer)
    {
        //Timer timerWrite;
        writer.write_at(buffer.data.get(), buffer.offset, buffer.fileOffset + buffer.processedCount);
        buffer.processedCount += buffer.offset;
        buffer.offset = 0;
        //timerWrite.print("Write buffer");
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
