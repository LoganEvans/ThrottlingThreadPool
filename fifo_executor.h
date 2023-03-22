#pragma once

#include "executor.h"
#include "task.h"

namespace theta {

class FIFOExecutorImpl : public Executor::Impl {
  friend class ScalingThreadpool;

 public:
  ~FIFOExecutorImpl() override;

  void post(Func func) override;

  FIFOExecutorImpl(const Executor::Opts& opts) : Executor::Impl(opts) {}

 protected:
  Func pop() override;

 private:
  TaskQueue queue_;
};

}  // namespace theta
