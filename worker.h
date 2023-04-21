#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <thread>

#include "epoch.h"
#include "task.h"

namespace theta {

class ThrottlingThreadpool;
class Task;

class Worker {
 public:
  Worker(TaskQueue* run_queue, NicePriority priority);
  ~Worker();

  void shutdown();

  NicePriority nice_priority() const;
  void set_nice_priority(NicePriority priority);
  pthread_t get_pthread();

 private:
  TaskQueue* run_queue_;
  std::mutex priority_mutex_;
  std::atomic<NicePriority> priority_;
  std::thread thread_;

  void run_loop();
};

}  // namespace theta
