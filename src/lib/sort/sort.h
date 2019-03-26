#pragma once

#include "../record.h"

void sort_inmemory(const Record* input, size_t size, const std::string& outfile, size_t threads);
void sort_external(const std::string& infile, size_t size, const std::string& outfile, size_t threads);
