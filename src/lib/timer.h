#pragma once

#include <chrono>
#include <ctime>
#include <iostream>

using TimerClock = std::chrono::steady_clock;

class Timer
{
public:
    Timer()
    {
        this->start();
    }

    void start()
    {
        this->point = TimerClock::now();
    }
    double get()
    {
        auto elapsed = TimerClock::now() - this->point;
        return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    }
    double add()
    {
        this->total += this->get();
        return this->total;
    }
    void reset()
    {
        this->total = 0;
        this->start();
    }

    void print(const char* name)
    {
        std::cerr << name << ": " << this->get() << std::endl;
    }

    double total = 0.0;

    std::chrono::time_point<TimerClock> point;
};
