#include "task.h"

#include <mutex>

namespace theta {
void TaskQueue::push(std::shared_ptr<Task> task) {
  {
    std::unique_lock lock{shared_mutex_};
    queue_.push_back(std::move(task));
  }

  sem_.release();
}

std::shared_ptr<Task> TaskQueue::pop() {
  if (!sem_.try_acquire()) {
    return nullptr;
  }
  return pop_impl();
}

std::shared_ptr<Task> TaskQueue::pop_blocking() {
  sem_.acquire();
  return pop_impl();
}

void TaskQueue::unblock_workers(size_t n) {
  sem_.release(n);
}

std::shared_ptr<Task> TaskQueue::pop_impl() {
  std::shared_ptr<Task> task_ptr{nullptr};

  std::unique_lock lock{shared_mutex_};
  if (!queue_.empty()) {
    task_ptr = std::move(queue_.front());
    queue_.pop_front();
  }

  return task_ptr;
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

void TaskQueues::push(NicePriority priority, std::shared_ptr<Task> task) {
  queue(priority)->push(std::move(task));
}

std::shared_ptr<Task> TaskQueues::pop_blocking(NicePriority priority) {
  return queue(priority)->pop_blocking();
}
}
