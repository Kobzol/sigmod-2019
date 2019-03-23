#pragma once

#include "../record.h"

#include <ostream>

void write_buffered(
        const Record *records, const SortRecord *sorted, size_t count,
        const std::string &output, size_t buffer_size, size_t threads
);

void write_mmap(
        const Record *records, const SortRecord *sorted, size_t count,
        const std::string &output, size_t threads
);
