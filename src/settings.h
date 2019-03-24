#pragma once

#define GIB(x) (x * 1024 * 1024 * 1024ull)

// maximum size of input that will be sorted in-memory
#define LIMIT_IN_MEMORY_SORT GIB(24)

// number of records to buffer before writing to the output
#define WRITE_BUFFER_COUNT (20 * 1024 * 1024ull)

// number of partially sorted results in external sort
#define EXTERNAL_SORT_PARTIAL_COUNT 50000000ull
//#define EXTERNAL_SORT_PARTIAL_COUNT 50000ull
