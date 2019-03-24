#include "radix.h"
#include <vector>

#include <cassert>
#include <atomic>
#include <byteswap.h>

#include "../thirdparty/kxsort.h"
#include "../compare.h"

struct RadixTraitsRowSortRecord
{
    static const int nBytes = KEY_SIZE;

    int kth_byte(const SortRecord& x, int k) {
        return x.header[KEY_SIZE - 1 - k] & ((unsigned char) 0xFF);
    }
    bool compare(const SortRecord& lhs, const SortRecord& rhs) {
        return lhs.header < rhs.header;
    }
};

void lsd_radix_sort(SortRecord* data, size_t size)
{
    int passes = 10;

    std::vector<SortRecord> buffer(size);

    SortRecord* __restrict__ active = data;
    SortRecord* __restrict__ next = buffer.data();

    const int buckets = 256;

    std::vector<int> counts(static_cast<size_t>(passes * buckets));

    for (int i = 0; i < static_cast<int>(size); i++)
    {
        for (int pass = passes - 1; pass >= 0; pass--)
        {
            unsigned char radix = active[i].header[pass];
            counts[pass * buckets + radix]++;
        }
    }

    int offsets[buckets];
    for (int pass = passes - 1; pass >= 0; pass--)
    {
        auto* __restrict__ count = counts.data() + buckets * pass;
        offsets[0] = 0;
        for (int i = 1; i < buckets; i++)
        {
            offsets[i] = offsets[i - 1] + count[i - 1];
        }

        for (int i = 0 ; i < static_cast<int>(size); i++)
        {
            auto& index = active[i];
            unsigned char radix = index.header[pass];
            auto target = offsets[radix]++;
            next[target] = index;
        }

        std::swap(active, next);
    }

    assert(active == data);
}

void msd_radix_sort(SortRecord* data, size_t size)
{
    kx::radix_sort(data, data + size, RadixTraitsRowSortRecord());
}
