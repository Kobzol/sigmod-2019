#include <iostream>
#include "util.h"

size_t file_size(FILE* file)
{
    auto pos = ftell(file);
    CHECK_NEG_ERROR(pos);
    CHECK_NEG_ERROR(fseek(file, 0, SEEK_END));
    auto size = ftell(file);
    CHECK_NEG_ERROR(size);
    CHECK_NEG_ERROR(fseek(file, pos, SEEK_SET));
    return static_cast<size_t>(size);
}
