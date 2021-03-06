#pragma once

#include <vector>

#include "../record.h"
#include "../../settings.h"

struct GroupData
{
    uint32_t start;
    uint32_t count;
};

struct FileRecord
{
    std::string name;
    size_t count = 0;
};

struct OverlapRange {
    OverlapRange(size_t start, size_t end, size_t offset, bool memory = true)
    : start(start), end(end), offset(offset), memory(memory)
    {

    }

    size_t count() const
    {
        return this->end - this->start;
    }

    size_t start; // read address from input data
    size_t end;   // end of iteration per thread
    size_t offset = 0;  // current iteration per thread
    bool memory;
};

struct GroupTarget
{
    uint32_t index;
    uint8_t group;
} __attribute__((packed));

void sort_inmemory(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
void sort_inmemory_overlapped(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
void sort_inmemory_distribute(const std::string& infile, size_t size, const std::string& outfile, size_t threads);

void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
void sort_external_records(const std::string& infile, size_t size, const std::string& outfile, size_t threads);

std::vector<GroupData> sort_records(const Record* __restrict__ input, SortRecord* __restrict__ output,
                                    GroupTarget* targets,
                                    ssize_t count, size_t threads);
void sort_records_copy(const Record* input,
        Record* target,
        GroupTarget* targets,
        SortRecord* output, ssize_t count, size_t threads);
std::vector<GroupData> sort_records_direct(const SortRecord* input, SortRecord* output,
                                    ssize_t count, size_t threads);
std::vector<std::vector<SortRecord>> sort_records_per_parts(const Record* __restrict__ input,
                                                            ssize_t count, size_t threads);

template <typename T>
inline std::vector<size_t> prefix_sum(const std::vector<std::vector<T>>& data)
{
    std::vector<size_t> starts;
    starts.reserve(data.size());
    starts.push_back(0);

    for (size_t i = 1; i < data.size(); i++)
    {
        starts.push_back(starts[i - 1] + data[i - 1].size());
    }

    return starts;
}
