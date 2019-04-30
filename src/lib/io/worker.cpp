#include "worker.h"
#include "../sort/buffer.h"

#include <atomic>

extern std::atomic<size_t> bufferIORead;
extern std::atomic<size_t> bufferIOWrite;

std::thread ioWorker(SyncQueue<IORequest>& ioQueue)
{
    return std::thread([&ioQueue]() {
        size_t lastWrite = 0;
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
                    std::cerr << "Read " << request.count << " in " << timerIO.get() << " ms" << std::endl;
                    bufferIORead += timerIO.get();
                }
                else if (request.type == IORequest::Type::ReadBuffer)
                {
                    request.readBuffer->read_from_source(request.count);
                    std::cerr << "Read buffer " << request.count << " in " << timerIO.get() << " ms" << std::endl;
                }
                else if (request.type == IORequest::Type::Write)
                {
                    request.writer->write_at(request.buffer, request.count, request.offset);
                    bufferIOWrite += timerIO.get();
                }
                else
                {
                    request.writer->write_discard(request.buffer, request.count, request.offset, lastWrite, 5);
                    lastWrite = request.count;
                    bufferIOWrite += timerIO.get();
                }
            }
            request.queue->push(request.count);
        }
    });
}
