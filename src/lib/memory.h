#pragma once

#include <sys/mman.h>
#include "util.h"

template <typename T>
class HugePageBuffer
{
public:
    HugePageBuffer()
    {

    }
    explicit HugePageBuffer(size_t count): count(count)
    {
        this->allocate(count);
    }
    HugePageBuffer(HugePageBuffer&& other)
    {
        this->data = other.data;
        this->count = other.count;
        other.data = nullptr;
    }
    DISABLE_COPY(HugePageBuffer);

    ~HugePageBuffer()
    {
        if (this->data)
        {
            CHECK_NEG_ERROR(munmap(this->data, this->count * sizeof(T)));
        }
    }

    T* get() const
    {
        return this->data;
    }

    void allocate(size_t count)
    {
        assert(this->data == nullptr);
        this->data = static_cast<T*>(mmap64(nullptr, count * sizeof(T), PROT_READ | PROT_WRITE,
                                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        CHECK_NEG_ERROR((ssize_t) this->data);
        CHECK_NEG_ERROR(madvise(this->data, count * sizeof(T), MADV_HUGEPAGE));
        this->count = count;
    }

    void trim(size_t newCount)
    {
        if (newCount < this->count)
        {
            CHECK_NEG_ERROR(mremap(
                    this->data, this->count * sizeof(T),
                    newCount * sizeof(T),
                    0));
            this->count = newCount;
        }
    }

    void deallocate()
    {
        CHECK_NEG_ERROR(munmap(this->data, this->count * sizeof(T)));
        this->data = nullptr;
    }

private:
    T* data = nullptr;
    size_t count = 0;
};
