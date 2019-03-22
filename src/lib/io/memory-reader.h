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
        FILE* file = fopen(path, "rb");
        CHECK_NULL_ERROR(file);
        this->size = file_size(file);

        this->data = std::unique_ptr<Record[]>(new Record[this->size / TUPLE_SIZE]);
        if (fread(this->data.get(), this->size, 1, file) == 0) assert(false);

        CHECK_NEG_ERROR(fclose(file));
    }
    DISABLE_COPY(MemoryReader);
    DISABLE_MOVE(MemoryReader);

    size_t get_size() const
    {
        return this->size;
    }

    const Record* get_data() const
    {
        return this->data.get();
    }

private:
    size_t size;
    std::unique_ptr<Record[]> data;
};
