#pragma once

#include <fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include <cstring>
#include <unistd.h>

#include "../util.h"
#include "../record.h"

template <bool Populate = false>
class MmapWriter {
public:
    explicit MmapWriter(const char* path, size_t count): size(count * TUPLE_SIZE)
    {
        FILE* file = fopen(path, "wb+");
        CHECK_NULL_ERROR(file);
        int fd = fileno(file);
        CHECK_NEG_ERROR(fd);
        CHECK_NEG_ERROR(ftruncate64(fd, this->size));

        if (Populate)
        {
            this->data = reinterpret_cast<Record*>(mmap64(nullptr, this->size, PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, 0));
        }
        else this->data = reinterpret_cast<Record*>(mmap64(nullptr, this->size, PROT_WRITE, MAP_SHARED, fd, 0));
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

    void expect_random()
    {
        CHECK_NEG_ERROR(madvise(this->data, this->size, MADV_RANDOM));
    }

private:
    size_t size;
    Record* data;
};
