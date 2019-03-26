#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <cstring>
#include <unistd.h>

#include "../util.h"
#include "../record.h"


class MmapWriter {
public:
    explicit MmapWriter(const char* path, size_t size): size(size)
    {
        FILE* file = fopen(path, "wb+");
        CHECK_NULL_ERROR(file);
        int fd = fileno(file);
        CHECK_NEG_ERROR(fd);
        CHECK_NEG_ERROR(ftruncate64(fd, size));

        this->data = reinterpret_cast<Record*>(mmap(nullptr, size, PROT_WRITE, MAP_SHARED, fd, 0));
        CHECK_NEG_ERROR((ssize_t) this->data);
        CHECK_NEG_ERROR(fclose(file));
    }
    ~MmapWriter()
    {
        Timer timerUnmap;
        CHECK_NEG_ERROR(munmap(this->data, this->size));
        timerUnmap.print("Unmap");
    }
    DISABLE_COPY(MmapWriter);
    DISABLE_MOVE(MmapWriter);

    Record* get_data() const
    {
        return this->data;
    }

    void write(const Record* data, size_t size = 0)
    {
        if (size == 0) size = this->size;
        std::memcpy(this->data, data, size);
    }

private:
    size_t size;
    Record* data;
};
