#pragma once

#include <mutex>
#include <queue>

#include "epoch.h"
#include "executor.h"
#include "task.h"

namespace theta {

class FIFOExecutorImpl : public ExecutorImpl {
  friend class ScalingThreadpool;

 public:
  ~FIFOExecutorImpl() override;

  void post(Func func) override;

  FIFOExecutorImpl(const Executor::Opts& opts)
      : ExecutorImpl(opts), fast_queue_(QueueOpts{}) {}

 protected:
  std::optional<EpochPtr<Task>> maybe_pop() override;

 private:
  Queue<Task> fast_queue_;

  std::mutex mu_;
  std::queue<EpochPtr<Task>> queue_;
};

}  // namespace theta
