#include "task.h"

#include <mutex>

namespace theta {
void TaskQueue::push(Task::Func func) {
  {
    std::unique_lock lock{shared_mutex_};
    queue_.push(func);
  }

  sem_.release();
}

Task::Func TaskQueue::pop() {
  if (!sem_.try_acquire()) {
    return nullptr;
  }

  Func func{nullptr};
  {
    std::unique_lock lock{shared_mutex_};
    if (!queue_.empty()) {
      func = queue_.front();
      queue_.pop();
    }
  }

  return func;
}

Task::Func TaskQueue::pop_blocking() {
  sem_.acquire();

  Func func{nullptr};
  {
    std::unique_lock lock{shared_mutex_};
    if (!queue_.empty()) {
      func = queue_.front();
      queue_.pop();
    }
  }

  return func;
}

void TaskQueue::unblock_workers(size_t n) {
  sem_.release(n);
}

TaskQueue* TaskQueues::queue(NicePriority priority) {
  switch (priority) {
    case NicePriority::kThrottled:
      return &throttled_queue_;
    case NicePriority::kRunning:
      return &running_queue_;
    default:
      return &prioritized_queue_;
  }
}

void TaskQueues::push(NicePriority priority, Task::Func func) {
  queue(priority)->push(func);
}

Task::Func TaskQueues::pop_blocking(NicePriority priority) {
  return queue(priority)->pop_blocking();
}

}
