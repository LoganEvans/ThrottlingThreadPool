#include "fifo_executor.h"

#include <glog/logging.h>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  if (maybe_run_immediately(func)) {
    return;
  }

  queue_.push(func);
}

Executor::Func FIFOExecutorImpl::pop() {
  printf("> FIFOExecutorImpl::pop()\n");

  return queue_.pop();
}

}  // namespace theta
