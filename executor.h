#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "task.h"
#include "worker.h"

namespace theta {

class NotImplemented : public std::logic_error {
 public:
  NotImplemented() : std::logic_error("Function not implemented") {}
};

enum class PriorityPolicy {
  FIFO,
  LIFO,
  EarliestDeadlineFirst,
  ExplicitPriority,
};

class ScalingThreadpool;
class Executor;
class FIFOExecutorImpl;

class ExecutorStats {
 public:
  ExecutorStats(bool run_state_is_normal);

  bool run_state_is_normal() const;

  size_t num_running() const;
  size_t num_waiting() const;
  size_t num_throttled() const;
  size_t num_finished() const;

  size_t limit_running() const;
  void set_limit_running(size_t val);

  size_t limit_throttled() const;

  bool transition(int running_delta, int throttled_delta, int waiting_delta,
                  int finished_delta);

 private:
  const bool run_state_is_normal_;

  std::atomic<size_t> num_running_{0};
  std::atomic<size_t> num_waiting_{0};
  std::atomic<size_t> num_throttled_{0};
  std::atomic<size_t> num_finished_{0};

  std::atomic<size_t> limit_running_{0};
  std::atomic<size_t> limit_throttled_{0};
};

class ExecutorOpts {
  friend class Executor;
  friend class ExecutorImpl;
  friend class ScalingThreadpool;

 public:
  PriorityPolicy priority_policy() const { return priority_policy_; }
  ExecutorOpts& set_priority_policy(PriorityPolicy val) {
    priority_policy_ = val;
    return *this;
  }

  size_t thread_weight() const { return thread_weight_; }
  ExecutorOpts& set_thread_weight(size_t val) {
    thread_weight_ = val;
    return *this;
  }

  size_t worker_limit() const { return worker_limit_; }
  ExecutorOpts& set_worker_limit(size_t val) {
    worker_limit_ = val;
    return *this;
  }

  bool require_low_latency() const { return require_low_latency_; }
  ExecutorOpts& set_require_low_latency(bool val) {
    require_low_latency_ = val;
    return *this;
  }

 protected:
  ScalingThreadpool* threadpool() const { return threadpool_; }
  ExecutorOpts& set_threadpool(ScalingThreadpool* val) {
    threadpool_ = val;
    return *this;
  }

  std::function<std::shared_ptr<Task>(ScalingThreadpool*, ExecutorStats*,
                                      std::shared_ptr<Task>)>
  maybe_run_immediately_callback() const {
    return maybe_run_immediately_callback_;
  }
  ExecutorOpts& set_maybe_run_immediately_callback(
      std::function<std::shared_ptr<Task>(ScalingThreadpool*, ExecutorStats*,
                                          std::shared_ptr<Task>)>
          val) {
    maybe_run_immediately_callback_ = val;
    return *this;
  }

 private:
  PriorityPolicy priority_policy_{PriorityPolicy::FIFO};
  size_t thread_weight_{1};
  size_t worker_limit_{0};
  bool require_low_latency_{false};
  ScalingThreadpool* threadpool_{nullptr};
  std::function<std::shared_ptr<Task>(ScalingThreadpool*, ExecutorStats*,
                                      std::shared_ptr<Task>)>
      maybe_run_immediately_callback_{nullptr};
};

class ExecutorImpl {
  friend class ScalingThreadpool;

 public:
  using Func = Task::Func;
  using Opts = ExecutorOpts;
  using Clock = std::chrono::high_resolution_clock;

  virtual ~ExecutorImpl() {}
  ExecutorImpl(Opts opts)
      : opts_(std::move(opts)),
        stats_(/*run_state_is_normal=*/!opts_.require_low_latency()) {
    stats_.set_limit_running(opts_.thread_weight());
  }

  const Opts& opts() const { return opts_; }

  virtual void post(Func func) { throw NotImplemented{}; }

  virtual void post(Func func, int priority) { throw NotImplemented{}; }

  virtual void post(Func func, std::chrono::time_point<Clock> deadline,
                    Func expireCallback = nullptr) {
    throw NotImplemented{};
  }

  virtual std::shared_ptr<Task> pop() = 0;

  ExecutorStats* stats() { return &stats_; }

 protected:
  std::shared_ptr<Task> maybe_run_immediately(std::shared_ptr<Task> task);

 private:
  const Opts opts_;

  ExecutorStats stats_;
};

class Executor {
 public:
  friend class ScalingThreadpool;

  using Clock = ExecutorImpl::Clock;
  using Func = Task::Func;
  using Opts = ExecutorOpts;

  template <typename ExecutorImplType>
  static Executor create(Opts opts);

  virtual ~Executor() {}

  const Opts& opts() { return impl_->opts(); }

  void post(Func func) { return impl_->post(func); }

  void post(Func func, int priority) { return impl_->post(func, priority); }

  void post(Func func, std::chrono::time_point<Clock> deadline,
            Func expireCallback = nullptr) {
    return impl_->post(func, deadline, expireCallback);
  }

 private:
  Executor(ExecutorImpl* impl);

  ExecutorImpl* impl_;
};

}  // namespace theta
