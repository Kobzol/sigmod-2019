#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <memory>
#include <sys/sendfile.h>

#include "../util.h"
#include "../record.h"
#include "memory-reader.h"

class FileWriter {
public:
    explicit FileWriter(const char* path, bool direct = false)
    {
        uint32_t mode = O_WRONLY | O_CREAT;
        if (direct)
        {
            mode |= O_DIRECT;
        }
        this->file = open(path, mode, 0666);
        CHECK_NEG_ERROR(this->file);
    }
    ~FileWriter()
    {
        CHECK_NEG_ERROR(close(this->file));
    }
    DISABLE_COPY(FileWriter);
    DISABLE_MOVE(FileWriter);

    void preallocate(size_t count)
    {
        CHECK_NEG_ERROR(ftruncate64(this->file, count * TUPLE_SIZE));
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
    void writeout(size_t count, size_t offset)
    {
        CHECK_NEG_ERROR(sync_file_range(this->file, offset * TUPLE_SIZE, count * TUPLE_SIZE, SYNC_FILE_RANGE_WRITE));
    }
    void discard(size_t count, size_t offset)
    {
        CHECK_NEG_ERROR(sync_file_range(this->file, offset * TUPLE_SIZE, count * TUPLE_SIZE,
                                        SYNC_FILE_RANGE_WAIT_BEFORE |
                                        SYNC_FILE_RANGE_WRITE |
                                        SYNC_FILE_RANGE_WAIT_AFTER));
        CHECK_NEG_ERROR(posix_fadvise(this->file, offset * TUPLE_SIZE, count * TUPLE_SIZE, POSIX_FADV_DONTNEED));
    }

    void write_discard(Record* record, size_t count, ssize_t offset, size_t previousCount, size_t discardWindow)
    {
        this->write_at(record, count, offset);
        writeout(count, offset);

        ssize_t discardOffset = offset - discardWindow * previousCount;
        if (discardOffset >= 0)
        {
            discard(previousCount, discardOffset);
        }
    }

private:
    int file;
};

