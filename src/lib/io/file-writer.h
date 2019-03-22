#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <memory>

#include "../util.h"
#include "../record.h"

class FileWriter {
public:
    explicit FileWriter(const char* path, size_t size) : size(size)
    {
        this->file = fopen(path, "wb");
        CHECK_NULL_ERROR(this->file);
    }
    ~FileWriter()
    {
        CHECK_NEG_ERROR(fflush(this->file));
        CHECK_NEG_ERROR(fclose(this->file));
    }
    DISABLE_COPY(FileWriter);
    DISABLE_MOVE(FileWriter);

    void write(const Record* data, size_t size = 0)
    {
        if (size == 0) size = this->size;
        CHECK_NEG_ERROR(fwrite(data, size, 1, this->file));
    }

private:
    size_t size;
    FILE* file;
};
