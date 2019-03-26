#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>

#include "../util.h"
#include "../record.h"

class MmapReader {
public:
    MmapReader() = default;

    explicit MmapReader(const char* path)
    {
        this->file = fopen(path, "rb");
        CHECK_NULL_ERROR(this->file);
        this->size = file_size(file);
        this->mmapData = this->readData = reinterpret_cast<Record*>(mmap(nullptr, this->size, PROT_READ, MAP_SHARED, fileno(this->file), 0));
        CHECK_NEG_ERROR((ssize_t) this->mmapData);
    }
    explicit MmapReader(const char* path, size_t start, size_t size): size(size)
    {
        size_t offset = 0;
        if (start % 4096 != 0)
        {
            offset = start % 4096;
            start -= offset;
        }

        this->file = fopen(path, "rb");
        CHECK_NULL_ERROR(file);
        this->mmapData = reinterpret_cast<Record*>(mmap64(nullptr, this->size, PROT_READ, MAP_SHARED, fileno(this->file), start));
        CHECK_NEG_ERROR((ssize_t) this->mmapData);
        this->readData = reinterpret_cast<Record*>(reinterpret_cast<char*>(this->mmapData) + offset);
    }
    MmapReader(MmapReader&& other) noexcept
    {
        this->file = other.file;
        this->mmapData = other.mmapData;
        this->readData = other.readData;
        this->size = other.size;
        other.file = nullptr;
        other.mmapData = nullptr;
        other.readData = nullptr;
        other.size = 0;
    }
    ~MmapReader()
    {
        if (this->mmapData)
        {
            CHECK_NEG_ERROR(munmap(this->mmapData, this->size));
        }
        if (this->file)
        {
            CHECK_NEG_ERROR(fclose(file));
        }
    }
    DISABLE_COPY(MmapReader);

    size_t get_size() const
    {
        return this->size;
    }

    const Record* get_data() const
    {
        return this->readData;
    }

private:
    FILE* file = nullptr;
    size_t size = 0;
    Record* mmapData = nullptr;
    Record* readData = nullptr;
};
