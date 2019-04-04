#pragma once

#include <vector>

#include "../record.h"
#include "../../settings.h"

struct GroupData
{
    uint32_t start;
    uint32_t count;
};

struct MergeRange {
public:
    size_t size() const
    {
        size_t size = 0;
        for (auto& group: this->groups)
        {
            size += group.count;
        }
        return size;
    }

    std::vector<GroupData> groups;
    size_t writeStart = 0;
};


void sort_inmemory(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
std::vector<GroupData> sort_records(const Record* input, SortRecord* output,
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
