#include "util.h"
#include "compare.h"

#include <unistd.h>


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
size_t file_size(int handle)
{
    auto pos = lseek64(handle, 0, SEEK_CUR);
    CHECK_NEG_ERROR(pos);
    CHECK_NEG_ERROR(lseek64(handle, 0, SEEK_END));
    auto size = lseek64(handle, 0, SEEK_CUR);
    CHECK_NEG_ERROR(size);
    CHECK_NEG_ERROR(lseek64(handle, pos, SEEK_SET));
    return static_cast<size_t>(size);
}

bool is_sorted(const Record* records, size_t count)
{
    for (size_t i = 1; i < count; i++)
    {
        if (!cmp_record(records[i - 1], records[i])) return false;
    }
    return true;
}
