#pragma once

#include "epoch.h"
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
  EpochPtr<Task> maybe_pop() override;

 private:
  TaskQueue queue_{NicePriority::kNone};
};

}  // namespace theta
