#pragma once

#include "executor.h"
#include "task.h"

namespace theta {

class FIFOExecutorImpl : public ExecutorImpl {
  friend class ScalingThreadpool;

 public:
  ~FIFOExecutorImpl() override;

  void post(Func func) override;

  FIFOExecutorImpl(const Executor::Opts& opts) : ExecutorImpl(opts) {}

 protected:
  std::shared_ptr<Task> maybe_pop() override;

 private:
  TaskQueue queue_{NicePriority::kNone};
};

}  // namespace theta
