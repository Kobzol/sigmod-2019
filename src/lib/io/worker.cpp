#include "worker.h"
#include "../sort/buffer.h"

#include <atomic>

extern std::atomic<size_t> bufferIORead;
extern std::atomic<size_t> bufferIOWrite;

std::thread ioWorker(SyncQueue<IORequest>& ioQueue)
{
    return std::thread([&ioQueue]() {
        while (true)
        {
            auto request = ioQueue.pop();
            if (request.isLast())
            {
                break;
            }
            else if (request.count)
            {
                Timer timerIO;
                if (request.type == IORequest::Type::Read)
                {
                    request.reader->read_at(request.buffer, request.count, request.offset);
                    bufferIORead += timerIO.get();
                }
                else if (request.type == IORequest::Type::ReadBuffer)
                {
                    request.readBuffer->read_from_source(request.count);
                }
                else
                {
                    request.writer->write_at(request.buffer, request.count, request.offset);
                    bufferIOWrite += timerIO.get();
                }
            }
            request.queue->push(request.count);
        }
    });
}
