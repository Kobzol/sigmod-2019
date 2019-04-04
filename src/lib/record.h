#pragma once

#include <array>
#include <cstdint>

const size_t TUPLE_SIZE = 100;
const size_t KEY_SIZE = 10;

using Record = std::array<uint8_t, TUPLE_SIZE>;
using Header = std::array<uint8_t, KEY_SIZE>;

struct SortRecord {
    SortRecord() = default;
    SortRecord(const Header& header, uint32_t index): header(header), index(index)
    {

    }

    Header header;
    uint32_t index;
} __attribute__((packed));
