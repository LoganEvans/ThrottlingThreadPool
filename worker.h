#pragma once

#include <atomic>
#include <thread>

#include "task.h"

namespace theta {

class ScalingThreadpool;

class Worker {
 public:
  using Func = Task::Func;

  Worker(TaskQueues* queues, TaskQueues::NicePriority priority);
  ~Worker();

  void shutdown();

  TaskQueues::NicePriority nice_priority() const;
  void set_nice_priority(TaskQueues::NicePriority priority);

 private:
  TaskQueues* queues_;
  std::atomic<TaskQueues::NicePriority> priority_;
  std::thread thread_;
  std::atomic<bool> shutdown_{false};

  void run_loop();
};

}  // namespace theta
