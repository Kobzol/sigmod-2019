#pragma once

#include "sort.h"
#include "../io/file-writer.h"
#include "../io/memory-reader.h"

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

void merge_range(std::vector<FileRecord>& files, std::vector<MemoryReader>& readers,
                 const MergeRange& range, FileWriter& writer);
void merge_files(std::vector<FileRecord>& files, const std::vector<MergeRange>& ranges,
                 const std::string& outfile, size_t size, size_t threads);

void compute_write_offsets(std::vector<MergeRange>& ranges);
std::vector<MergeRange> remove_empty_ranges(const std::vector<MergeRange>& ranges);
