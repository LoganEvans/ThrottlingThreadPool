#pragma once

#include <atomic>
#include <thread>

#include "task.h"

namespace theta {

class ScalingThreadpool;
class Task;

class Worker {
 public:
  Worker(TaskQueues* queues, NicePriority priority);
  ~Worker();

  void shutdown();

  NicePriority nice_priority() const;
  void set_nice_priority(NicePriority priority);
  pthread_t get_pthread();

 private:
  TaskQueues* queues_;
  std::atomic<NicePriority> priority_;
  std::thread thread_;
  std::atomic<bool> shutdown_{false};

  void run_loop();
};

}  // namespace theta
