#include "executor.h"

#include <glog/logging.h>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {
  std::lock_guard guard{mutex_};
  CHECK(queue_.empty());

  while (!queue_.empty()) {
    queue_.pop();
  }
}

void FIFOExecutorImpl::post(ExecutorImpl::Func func) {
  std::lock_guard guard{mutex_};

  queue_.push(func);
}

ExecutorImpl::Func FIFOExecutorImpl::pop() {
  printf("> FIFOExecutorImpl::pop()\n");
  std::lock_guard guard{mutex_};

  if (queue_.empty()) {
    return nullptr;
  }

  auto f = queue_.front();
  queue_.pop();
  return f;
}

size_t FIFOExecutorImpl::tasks_waiting() const {
  std::lock_guard guard{mutex_};

  return queue_.size();
}

}  // namespace theta
