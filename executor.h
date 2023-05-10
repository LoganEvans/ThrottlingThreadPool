#pragma once

#ifndef _GNU_SOURCE
// _GNU_SOURCE is required for getrusage.
static_assert(false);
#endif

#include <sys/resource.h>
#include <sys/time.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

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

class ThrottlingThreadpool;
class Executor;
class FIFOExecutorImpl;

class ExecutorStats {
 public:
  ExecutorStats()
      : active_(/*num_=*/0, /*limit_=*/10) {
    ema_last_update_.store(timeval{.tv_sec = 0, .tv_usec = 0},
                           std::memory_order::relaxed);
  }

  bool reserve_active();
  void unreserve_active();
  void set_active_limit(uint32_t val);
  std::pair<int, int> active_num_limit(
      std::memory_order mem_order = std::memory_order::relaxed) const;

  int waiting_num(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void waiting_delta(int val);

  int running_num(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void running_delta(int val);

  int throttled_num(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void throttled_delta(int val);

  int finished_num(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void finished_delta(int val);

  double ema_usage_proportion(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  double ema_nivcsw_per_task(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void update_ema(struct rusage* begin_ru, struct timeval* begin_tv,
                  struct rusage* end_ru, struct timeval* end_tv);

  std::string debug_string() const;

 //private:
  union Active {
    struct {
      std::atomic<uint32_t> num;
      std::atomic<uint32_t> limit;
    };
    std::atomic<uint64_t> line;

    Active(uint32_t num_, uint32_t limit_) : num(num_), limit(limit_) {}
    Active(uint64_t line_) : line(line_) {}
    Active(const Active& other)
        : Active(/*line_=*/other.line.load(std::memory_order::relaxed)) {}
    Active& operator=(const Active& other) {
      line.store(other.line.load(std::memory_order::relaxed));
      return *this;
    }
  } active_;
  static_assert(sizeof(Active) == sizeof(Active::line), "");

  std::atomic<uint32_t> waiting_num_{0};
  std::atomic<uint32_t> running_num_{0};
  std::atomic<uint32_t> throttled_num_{0};

  std::atomic<uint64_t> finished_num_{0};

  std::atomic<double> ema_usage_proportion_{1.0};
  std::atomic<double> ema_nivcsw_per_task_{0.0};
  std::atomic<timeval> ema_last_update_;
};

class ExecutorOpts {
  friend class Executor;
  friend class ExecutorImpl;
  friend class ThrottlingThreadpool;

 public:
  static constexpr size_t kNoWorkerLimit = 0;

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

 protected:
  TaskQueue<>* run_queue() const { return run_queue_; }
  ExecutorOpts& set_run_queue(TaskQueue<>* val) {
    run_queue_ = val;
    return *this;
  }

 private:
  PriorityPolicy priority_policy_{PriorityPolicy::FIFO};
  size_t thread_weight_{1};
  size_t worker_limit_{0};
  TaskQueue<>* run_queue_{nullptr};
};

class ExecutorImpl {
  friend class ThrottlingThreadpool;
  friend class Worker;
  friend class Task;

 public:
  using Func = Task::Func;
  using Opts = ExecutorOpts;
  using Clock = std::chrono::high_resolution_clock;

  static void get_tv(timeval* tv);

  virtual ~ExecutorImpl() {}

  ExecutorImpl(Opts opts)
      : opts_(std::move(opts)),
        throttle_list_(
            /*modification_queue_size=*/std::max(64UL, opts_.worker_limit())) {
    stats_.set_active_limit(opts_.worker_limit());
  }

  const Opts& opts() const { return opts_; }

  virtual void post(Func func) { throw NotImplemented{}; }

  virtual void post(Func func, int priority) { throw NotImplemented{}; }

  virtual void post(Func func, std::chrono::time_point<Clock> deadline,
                    Func expireCallback = nullptr) {
    throw NotImplemented{};
  }

  virtual std::unique_ptr<Task> pop() = 0;

  ExecutorStats* stats() { return &stats_; }
  const ExecutorStats* stats() const { return &stats_; }

 protected:
  void refill_queues(Task** take_first = nullptr);

 private:
  const Opts opts_;
  std::mutex mtx_;
  ThrottleList throttle_list_;

  ExecutorStats stats_;

  void refresh_limits();
};

class Executor {
 public:
  friend class ThrottlingThreadpool;

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
