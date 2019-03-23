#pragma once

#include <cstdlib>
#include <sstream>

#include "timer.h"
#include "record.h"

#define CHECK_NEG_ERROR(ret)\
if ((ret) < 0)\
{\
  std::stringstream err;\
  err << __FILE__ << ":" << __LINE__;\
  perror(err.str().c_str());\
  std::exit(1);\
}

#define CHECK_NULL_ERROR(ret)\
if ((ret) == nullptr)\
{\
  std::stringstream err;\
  err << __FILE__ << ":" << __LINE__;\
  perror(err.str().c_str());\
  std::exit(1);\
}

#define DISABLE_COPY(klass)\
klass(const klass&) = delete;\
klass operator=(const klass&) = delete;

#define DISABLE_MOVE(klass)\
klass(const klass&&) = delete;

#define TIME_BLOCK(name, block)\
Timer timer##__LINE__;\
block;\
timer##__LINE__.print(name);

size_t file_size(FILE* file);
bool is_sorted(const Record* records, size_t count);
