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
      : ExecutorImpl(opts),
        fast_post_queue_(QueueOpts{}),
        fast_pop_queue_(QueueOpts{}) {}

 protected:
  std::unique_ptr<Task> pop() override;

 private:
  Queue<Task*> fast_post_queue_;
  Queue<Task*> fast_pop_queue_;

  std::mutex mu_;
  std::queue<Task*> slow_queue_;

  void shuffle_fifo_queues(Task* task_to_post, Task** task_to_pop);
};

}  // namespace theta
