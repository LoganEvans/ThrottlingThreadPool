#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>

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

class ExecutorImpl {
  friend class ScalingThreadpool;
  friend class Executor;

 public:
  using Clock = std::chrono::high_resolution_clock;
  using Func = std::function<void()>;

  class Opts {
   public:
    PriorityPolicy priority_policy() const { return priority_policy_; }
    Opts& set_priority_policy(PriorityPolicy val) {
      priority_policy_ = val;
      return *this;
    }

    size_t thread_weight() const { return thread_weight_; }
    Opts& set_thread_weight(size_t val) {
      thread_weight_ = val;
      return *this;
    }

    size_t worker_limit() const { return worker_limit_; }
    Opts& set_worker_limit(size_t val) {
      worker_limit_ = val;
      return *this;
    }

    bool require_low_latency() const { return require_low_latency_; }
    Opts& set_require_low_latency(bool val) {
      require_low_latency_ = val;
      return *this;
    }

   private:
    PriorityPolicy priority_policy_{PriorityPolicy::FIFO};
    size_t thread_weight_{1};
    size_t worker_limit_{0};
    bool require_low_latency_{false};
  };

  virtual ~ExecutorImpl() {}

  virtual void post(Func func) { throw NotImplemented{}; }

  virtual void post(Func func, int priority) { throw NotImplemented{}; }

  virtual void post(Func func, std::chrono::time_point<Clock> deadline,
                    Func expireCallback = nullptr) {
    throw NotImplemented{};
  }

  virtual size_t tasks_waiting() const = 0;

 protected:
  const Opts& opts_;

  ExecutorImpl(const Opts& opts) : opts_(opts) {}

  virtual Func pop() = 0;
};

class ExecutorImplPool {
 public:
  static ExecutorImplPool& getInstance() {
    static ExecutorImplPool instance;
    return instance;
  }

  ExecutorImplPool(const ExecutorImplPool&) = delete;
  void operator=(const ExecutorImplPool&) = delete;

 private:
  ExecutorImplPool() {}
};

class FIFOExecutorImpl : public ExecutorImpl {
  friend class ScalingThreadpool;

 public:
  ~FIFOExecutorImpl() override;

  void post(Func func) override;

 protected:
  FIFOExecutorImpl(const Opts& opts) : ExecutorImpl(opts) {}

  Func pop() override;

  size_t tasks_waiting() const override;

 private:
  mutable std::mutex mutex_;
  std::queue<Func> queue_;
};

}  // namespace theta
