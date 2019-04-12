#pragma once

#define GIB(x) (x * 1024 * 1024 * 1024ull)

// maximum size of input that will be sorted in-memory
#define LIMIT_IN_MEMORY_SORT GIB(24)

// number of records to buffer before writing to the output
#define WRITE_BUFFER_COUNT (400 * 4096ull)

#ifdef REAL_RUN
    #define WRITE_LOCATION (std::string("/output-disk"))
// number of partially sorted results in external sort
    #define EXTERNAL_SORT_PARTIAL_COUNT 120000000ull
#else
    #define WRITE_LOCATION (std::string("/tmp"))
    #define EXTERNAL_SORT_PARTIAL_COUNT 5000000ull
#endif

// number of groups used for sort
#define SORT_GROUP_COUNT 256

// buffer sizes for external merges
#define MERGE_READ_BUFFER_COUNT (1024 * 512)
#define MERGE_WRITE_BUFFER_COUNT (1024 * 512)

// number of parts to split the read file into when doing inmemory overlapped sort
#define INMEMORY_OVERLAP_PARTS 8
