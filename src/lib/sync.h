#pragma once

#include <mutex>
#include <thread>
#include <condition_variable>
#include <queue>


template <typename T>
class SyncQueue {
public:
    void push(T buffer)
    {
        {
            std::unique_lock<std::mutex> lock(this->mutex);
            this->queue.push(buffer);
        }
        this->cond.notify_one();
    }

    T pop()
    {
        std::unique_lock<std::mutex> lock(this->mutex);

        this->cond.wait(lock, [this]() {
            return !this->queue.empty();
        });

        auto item = std::move(this->queue.front());
        this->queue.pop();

        return item;
    }

    void waitForEmpty()
    {
        while (!this->queue.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

private:
    std::mutex mutex;
    std::condition_variable cond;

    std::queue<T> queue;
};
