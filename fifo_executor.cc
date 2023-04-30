#include "fifo_executor.h"

#include <glog/logging.h>

#include <optional>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto* task = new Task{Task::Opts{}.set_func(func).set_executor(this)};

  task->set_state(Task::State::kQueuedExecutor);

  if (!fast_queue_.push_back(task)) {
    std::lock_guard l{mu_};
    slow_queue_.push(task);
  }

  refill_queues();
}

std::unique_ptr<Task> FIFOExecutorImpl::pop() {
  auto* t = fast_queue_.pop_front();
  if (t) {
    return std::unique_ptr<Task>{t};
  }

  std::lock_guard l{mu_};
  if (slow_queue_.empty()) {
    return nullptr;
  }

  auto* p = slow_queue_.front();
  slow_queue_.pop();

  // Refill the fast queue halfway.
  for (size_t i = 0; i < fast_queue_.capacity() / 2; i++) {
    if (slow_queue_.empty()) {
      break;
    }
    if (!fast_queue_.push_back(slow_queue_.front())) {
      slow_queue_.front() = t;
      break;
    }
    slow_queue_.pop();
  }

  return std::unique_ptr<Task>{p};
}

}  // namespace theta
