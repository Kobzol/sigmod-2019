#pragma once

#include "../record.h"

#include <ostream>

void write_buffered(
        const Record* records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t buffer_size, size_t threads
);

void write_sequential_io(
        const Record* records, const SortRecord* sorted, size_t count,
        const std::string& output, size_t buffer_size, size_t threads
);

void write_mmap(
        const Record* records, const uint32_t* sorted, ssize_t count,
        const std::string& output, size_t threads
);
