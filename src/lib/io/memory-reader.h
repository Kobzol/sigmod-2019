#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <memory>

#include "../util.h"
#include "../record.h"

class MemoryReader {
public:
    explicit MemoryReader(const char* path)
    {
        this->file = fopen(path, "rb");
        CHECK_NULL_ERROR(this->file);
        this->size = file_size(this->file);
    }
    DISABLE_COPY(MemoryReader);
    ~MemoryReader()
    {
        CHECK_NEG_ERROR(fclose(this->file));
    }

    MemoryReader(MemoryReader&& other) noexcept
    {
        this->data = std::move(other.data);
        this->size = other.size;
    }

    void read(size_t size)
    {
        this->data = std::unique_ptr<Record[]>(new Record[size / TUPLE_SIZE]);
        if (fread(this->data.get(), size, 1, file) == 0) assert(false);
    }

    size_t get_size() const
    {
        return this->size;
    }

    const Record* get_data() const
    {
        return this->data.get();
    }

private:
    FILE* file = nullptr;
    size_t size;
    std::unique_ptr<Record[]> data;
};
