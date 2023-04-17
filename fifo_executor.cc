#include "fifo_executor.h"

#include <glog/logging.h>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto task =
      EpochPtr<Task>::make(Task::Opts{}.set_func(func).set_executor(this));

  task->set_state(Task::State::kQueuedExecutor);
  queue_.push(std::move(task));
  refill_queues();
}

EpochPtr<Task> FIFOExecutorImpl::maybe_pop() {
  return queue_.maybe_pop().value_or(nullptr);
}

}  // namespace theta
