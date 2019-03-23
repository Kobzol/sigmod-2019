#include <iostream>
#include <byteswap.h>
#include "util.h"

size_t file_size(FILE* file)
{
    auto pos = ftell(file);
    CHECK_NEG_ERROR(pos);
    CHECK_NEG_ERROR(fseek(file, 0, SEEK_END));
    auto size = ftell(file);
    CHECK_NEG_ERROR(size);
    CHECK_NEG_ERROR(fseek(file, pos, SEEK_SET));
    return static_cast<size_t>(size);
}

static bool compare_lte(const Record* lhs, const Record* rhs)
{
    const uint64_t a = bswap_64(*reinterpret_cast<const uint64_t*>(&lhs[0]));
    const uint64_t b = bswap_64(*reinterpret_cast<const uint64_t*>(&rhs[0]));

    if (a == b)
    {
        const uint16_t c = bswap_16(*reinterpret_cast<const uint16_t*>(&lhs[8]));
        const uint16_t d = bswap_16(*reinterpret_cast<const uint16_t*>(&rhs[8]));
        return c <= d;
    }
    return a < b;
}

bool is_sorted(const Record* records, size_t count)
{
    for (size_t i = 1; i < count; i++)
    {
        if (!compare_lte(records + (i - 1), records + i)) return false;
    }
    return true;
}
