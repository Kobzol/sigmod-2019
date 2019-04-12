#pragma once

#include <fcntl.h>
#include <cassert>
#include <memory>
#include <unistd.h>

#include "../util.h"
#include "../record.h"

class MemoryReader {
public:
    explicit MemoryReader(const char* path)
    {
        this->handle = open(path, O_RDONLY);
        CHECK_NEG_ERROR(this->handle);
        this->size = file_size(this->handle);
    }
    DISABLE_COPY(MemoryReader);
    ~MemoryReader()
    {
        if (this->handle != -1)
        {
            CHECK_NEG_ERROR(close(this->handle));
        }
    }

    MemoryReader(MemoryReader&& other) noexcept
    {
        this->size = other.size;
        this->handle = other.handle;
        other.handle = -1;
    }

    void read(Record* data, size_t count)
    {
        size_t size = count * TUPLE_SIZE;
        size_t total = 0;
        char* buf = reinterpret_cast<char*>(data);
        while (total < size)
        {
            auto readSize = ::read(this->handle, buf + total, size - total);
            CHECK_NEG_ERROR(readSize);
            total += readSize;
        }
    }

    void read_at(Record* data, size_t count, size_t offset)
    {
        size_t totalRead = count * TUPLE_SIZE;
        size_t currentRead = 0;
        size_t readOffset = offset * TUPLE_SIZE;
        char* buf = reinterpret_cast<char*>(data);
        while (currentRead < totalRead)
        {
            auto readSize = ::pread64(this->handle, buf + currentRead,
                    totalRead - currentRead, readOffset + currentRead);
            CHECK_NEG_ERROR(readSize);
            currentRead += readSize;
        }
    }

    void readahead(size_t count)
    {
        CHECK_NEG_ERROR(::readahead(this->handle, 0, count * TUPLE_SIZE));
    }

    size_t get_size() const
    {
        return this->size;
    }

private:
    int handle = -1;
    size_t size;
};
