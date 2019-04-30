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
        Write,
        WriteDiscard,
        End
    };

    static IORequest read(Record* buffer,
                          size_t count,
                          size_t offset,
                          SyncQueue<size_t>* queue,
                          MemoryReader* reader)
    {
        IORequest req(Type::Read, buffer, count, offset, queue);
        req.reader = reader;
        return req;
    }
    static IORequest write(Record* buffer,
                          size_t count,
                          size_t offset,
                          SyncQueue<size_t>* queue,
                          FileWriter* writer)
    {
        IORequest req(Type::Write, buffer, count, offset, queue);
        req.writer = writer;
        return req;
    }
    static IORequest write_discard(Record* buffer,
                           size_t count,
                           size_t offset,
                           SyncQueue<size_t>* queue,
                           FileWriter* writer)
    {
        IORequest req(Type::WriteDiscard, buffer, count, offset, queue);
        req.writer = writer;
        return req;
    }
    static IORequest read_buffer(size_t count,
                          SyncQueue<size_t>* queue,
                          ReadBuffer* readBuffer)
    {
        IORequest req(Type::ReadBuffer, nullptr, count, 0, queue);
        req.readBuffer = readBuffer;
        return req;
    }

    static IORequest last()
    {
        return {};
    }

    bool isLast() const
    {
        return this->type == Type::End;
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

private:
    IORequest(): type(Type::End)
    {

    }
    IORequest(Type type,
            Record* buffer,
            size_t count,
            size_t offset,
            SyncQueue<size_t>* queue):
            buffer(buffer), count(count), offset(offset), queue(queue), type(type)
    {

    }
};

std::thread ioWorker(SyncQueue<IORequest>& ioQueue);
