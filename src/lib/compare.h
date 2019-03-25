#pragma once

#include <byteswap.h>

#include "record.h"

inline bool cmp_header(const Header& lhs, const Header& rhs)
{
    const uint64_t a = bswap_64(*reinterpret_cast<const uint64_t*>(&lhs[0]));
    const uint64_t b = bswap_64(*reinterpret_cast<const uint64_t*>(&rhs[0]));

    if (a == b)
    {
        const uint16_t c = bswap_16(*reinterpret_cast<const uint16_t*>(&lhs[8]));
        const uint16_t d = bswap_16(*reinterpret_cast<const uint16_t*>(&rhs[8]));
        return c < d;
    }
    return a < b;
}

inline bool cmp_record(const Record& lhs, const Record& rhs)
{
    return *(reinterpret_cast<const Header*>(&lhs)) < *(reinterpret_cast<const Header*>(&rhs));
}
