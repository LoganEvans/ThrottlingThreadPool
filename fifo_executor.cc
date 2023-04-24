#include "fifo_executor.h"

#include <glog/logging.h>

#include <optional>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto* task = new Task{Task::Opts{}.set_func(func).set_executor(this)};

  task->set_state(Task::State::kQueuedExecutor);
  task = fast_queue_.push_back(task);

  if (task) {
    std::lock_guard l{mu_};
    queue_.push(task);
  }

  refill_queues();
}

std::unique_ptr<Task> FIFOExecutorImpl::pop() {
  auto* t = fast_queue_.pop_front();
  if (t) {
    return std::unique_ptr<Task>{t};
  }

  std::lock_guard l{mu_};
  if (queue_.empty()) {
    return nullptr;
  }

  auto* p = std::move(queue_.front());
  queue_.pop();

  // Refill the fast queue halfway.
  for (size_t i = 0; i < fast_queue_.capacity() / 2; i++) {
    if (queue_.empty()) {
      break;
    }
    auto* t = fast_queue_.push_back(std::move(queue_.front()));
    if (t) {
      queue_.front() = t;
      break;
    }
    queue_.pop();
  }

  return std::unique_ptr<Task>{p};
}

}  // namespace theta
