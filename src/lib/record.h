#pragma once

#include <array>
#include <cstdint>

const int TUPLE_SIZE = 100;
const int KEY_SIZE = 10;

using Record = std::array<uint8_t, TUPLE_SIZE>;
using Header = std::array<uint8_t, KEY_SIZE>;

struct SortRecord {
    Header header;
    uint32_t index;
};
