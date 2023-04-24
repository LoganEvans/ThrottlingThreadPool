#pragma once

#include <memory>
#include <mutex>
#include <queue>

#include "executor.h"
#include "task.h"

namespace theta {

class FIFOExecutorImpl : public ExecutorImpl {
  friend class ThrottlingThreadpool;

 public:
  ~FIFOExecutorImpl() override;

  void post(Func func) override;

  FIFOExecutorImpl(const Executor::Opts& opts)
      : ExecutorImpl(opts), fast_queue_(QueueOpts{}) {}

 protected:
  std::unique_ptr<Task> pop() override;

 private:
  Queue<Task> fast_queue_;

  std::mutex mu_;
  std::queue<Task*> queue_;
};

}  // namespace theta
