#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <memory>
#include <unistd.h>

#include "../util.h"
#include "../record.h"
#include "memory-reader.h"

class FileWriter {
public:
    explicit FileWriter(const char* path)
    {
        this->file = open(path, O_WRONLY | O_CREAT, 0666);
        CHECK_NEG_ERROR(this->file);
    }
    ~FileWriter()
    {
        CHECK_NEG_ERROR(close(this->file));
    }
    DISABLE_COPY(FileWriter);
    DISABLE_MOVE(FileWriter);

    void preallocate(size_t size)
    {
        CHECK_NEG_ERROR(ftruncate64(this->file, size));
    }
    void seek(size_t offset)
    {
        CHECK_NEG_ERROR(lseek64(this->file, offset * TUPLE_SIZE, SEEK_SET));
    }

    void write(const Record* data, size_t count)
    {
        size_t size = count * TUPLE_SIZE;
        size_t total = 0;
        auto input = reinterpret_cast<const char*>(data);

        while (total < size)
        {
            ssize_t written = ::write(this->file, input + total, size - total);
            CHECK_NEG_ERROR(written);
            total += written;
        }
    }
    void write_at(const Record* data, size_t count, size_t offset)
    {
        size_t size = count * TUPLE_SIZE;
        offset *= TUPLE_SIZE;
        size_t total = 0;
        auto input = reinterpret_cast<const char*>(data);

        while (total < size)
        {
            ssize_t written = ::pwrite64(this->file, input + total, size - total, offset + total);
            CHECK_NEG_ERROR(written);
            total += written;
        }
    }

    void splice_from(MemoryReader& reader, size_t outOffset, size_t inOffset, size_t count)
    {
        auto size = count * TUPLE_SIZE;
        off64_t writeOffset = outOffset * TUPLE_SIZE;
        off64_t readOffset = inOffset * TUPLE_SIZE;
        size_t written = 0;
        while (written < size)
        {
            auto left = size - written;
            auto written_actual = copy_file_range(
                    reader.handle,
                    &readOffset,
                    this->file,
                    &writeOffset,
                    left,
                    0
            );
            CHECK_NEG_ERROR(written_actual);
            written += written_actual;
        }
    }

private:
    int file;
};
