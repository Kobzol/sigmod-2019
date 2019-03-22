#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>

#include "../util.h"
#include "../record.h"

class MmapReader {
public:
    explicit MmapReader(const char* path)
    {
        FILE* file = fopen(path, "rb");
        CHECK_NULL_ERROR(file);
        this->size = file_size(file);
        this->data = reinterpret_cast<Record*>(mmap(nullptr, this->size, PROT_READ, MAP_SHARED, fileno(file), 0));
        CHECK_NULL_ERROR(this->data);
        CHECK_NEG_ERROR(fclose(file));
    }
    ~MmapReader()
    {
        CHECK_NEG_ERROR(munmap(this->data, this->size));
    }
    DISABLE_COPY(MmapReader);
    DISABLE_MOVE(MmapReader);

    size_t get_size() const
    {
        return this->size;
    }

    const Record* get_data() const
    {
        return this->data;
    }

private:
    size_t size;
    Record* data;
};
