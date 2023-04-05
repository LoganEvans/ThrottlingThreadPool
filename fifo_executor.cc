#include "fifo_executor.h"

#include <glog/logging.h>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto task =
      std::make_shared<Task>(Task::Opts{}.set_func(func).set_executor(this));

  task->set_state(Task::State::kQueuedExecutor);
  queue_.push(std::move(task));
  refill_queues();
}

std::shared_ptr<Task> FIFOExecutorImpl::maybe_pop() {
  return queue_.maybe_pop();
}

}  // namespace theta
