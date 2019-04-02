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


std::vector<GroupData> sort_inmemory(const Record* input, size_t size, const std::string& outfile, size_t threads);
void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
