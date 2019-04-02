#pragma once

#define GIB(x) (x * 1024 * 1024 * 1024ull)

// maximum size of input that will be sorted in-memory
#define LIMIT_IN_MEMORY_SORT GIB(24)

// number of records to buffer before writing to the output
#define WRITE_BUFFER_COUNT (400 * 4096ull)

#ifdef REAL_RUN
    #define WRITE_LOCATION (std::string("/output-disk"))
// number of partially sorted results in external sort
    #define EXTERNAL_SORT_PARTIAL_COUNT 50000000ull
#else
    #define WRITE_LOCATION (std::string("/tmp"))
    #define EXTERNAL_SORT_PARTIAL_COUNT 5000000ull
#endif

#define SORT_GROUP_COUNT 64
#define MERGE_READ_BUFFER_COUNT 4096 * 6
#define MERGE_WRITE_BUFFER_COUNT 8192 * 32
