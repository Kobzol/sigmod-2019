#pragma once

#pragma once

#include <thread>
#include "../record.h"
#include "../sync.h"
#include "memory-reader.h"
#include "file-writer.h"

class ReadBuffer;
class WriteBuffer;

class IORequest {
public:
    enum class Type {
        Read,
        ReadBuffer,
        Write
    };

    IORequest(Record* buffer,
              size_t count,
              size_t offset,
              SyncQueue<size_t>* queue,
              MemoryReader* reader):
            buffer(buffer), count(count), offset(offset), queue(queue), reader(reader), type(Type::Read)
    {

    }
    IORequest(Record* buffer,
              size_t count,
              size_t offset,
              SyncQueue<size_t>* queue,
              FileWriter* writer):
            buffer(buffer), count(count), offset(offset), queue(queue), writer(writer), type(Type::Write)
    {

    }
    IORequest(size_t count,
              SyncQueue<size_t>* queue,
              ReadBuffer* readBuffer):
            count(count), queue(queue), readBuffer(readBuffer), type(Type::ReadBuffer)
    {

    }
    IORequest(): buffer(nullptr)
    {

    }

    static IORequest last()
    {
        return {};
    }

    bool isLast() const
    {
        return this->buffer == nullptr;
    }

    Record* buffer;
    size_t count;
    size_t offset;
    SyncQueue<size_t>* queue;

    union {
        MemoryReader* reader;
        FileWriter* writer;
        ReadBuffer* readBuffer;
    };
    Type type;
};

std::thread ioWorker(SyncQueue<IORequest>& ioQueue);
