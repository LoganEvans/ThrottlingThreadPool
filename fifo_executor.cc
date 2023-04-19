#include "fifo_executor.h"

#include <glog/logging.h>

#include <optional>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto task =
      EpochPtr<Task>::make(Task::Opts{}.set_func(func).set_executor(this));

  task->set_state(Task::State::kQueuedExecutor);

  auto v = fast_queue_.push_back(std::move(task));
  if (v.has_value()) {
    std::lock_guard l{mu_};
    queue_.push(std::move(v.value()));
  }

  refill_queues();
}

std::optional<EpochPtr<Task>> FIFOExecutorImpl::maybe_pop() {
  auto t = fast_queue_.pop_front();
  if (t.has_value()) {
    return std::move(t.value());
  }

  std::lock_guard l{mu_};
  if (queue_.empty()) {
    return {};
  }

  auto p = std::move(queue_.front());
  queue_.pop();
  return p;
}

}  // namespace theta
