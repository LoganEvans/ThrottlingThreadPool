#pragma once

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <semaphore>
#include <shared_mutex>
#include <thread>

#include "executor.h"

namespace theta {

class Executor;
class ExecutorImpl;

class ScalingThreadpool {
  friend class Executor;
  friend class ExecutorImpl;

 public:
  class ConfigureOpts {
   public:
    // The number of cores where the only running threadpool threads will have
    // a nice value of 19.
    size_t nice_cores() const { return nice_cores_; }
    ConfigureOpts& set_nice_cores(size_t val) {
      nice_cores_ = val;
      return *this;
    }

    size_t thread_limit() const { return thread_limit_; }
    ConfigureOpts& set_thread_limit(size_t val) {
      thread_limit_ = val;
      return *this;
    }

    std::chrono::milliseconds throttle_interval() const {
      return throttle_interval_;
    }
    ConfigureOpts& set_throttle_interval(std::chrono::milliseconds val) {
      throttle_interval_ = val;
      return *this;
    }

    static ConfigureOpts defaultOpts();

   private:
    size_t nice_cores_{0};
    size_t thread_limit_{0};
    std::chrono::milliseconds throttle_interval_{0};
  };

  static ScalingThreadpool& getInstance() {
    static ScalingThreadpool instance;
    return instance;
  }

  ~ScalingThreadpool();

  ScalingThreadpool(const ScalingThreadpool&) = delete;
  void operator=(const ScalingThreadpool&) = delete;

  void configure(const ConfigureOpts& opts);

 private:
  ScalingThreadpool();

  ExecutorImpl* create(const ExecutorImpl::Opts& opts);
  void task_loop();
  void notify_new_task();

  std::shared_mutex shared_mutex_;
  ConfigureOpts opts_;

  std::vector<std::thread> threads_;
  std::atomic<bool> shutdown_{false};
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> high_prio_sem_{
      0};
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> normal_prio_sem_{
      0};
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> low_prio_sem_{0};

  std::vector<std::unique_ptr<ExecutorImpl>> executors_;
};

class Executor {
  friend class ScalingThreadpool;

 public:
  using Clock = ExecutorImpl::Clock;
  using Func = ExecutorImpl::Func;
  using Opts = ExecutorImpl::Opts;

  class Stats {
   public:
    size_t tasks_running() const;
    size_t tasks_waiting() const;
    size_t tasks_throttled() const;
    size_t tasks_finished() const;

    bool transition(int running_delta, int throttled_delta, int waiting_delta,
                    int finished_delta);

   private:
    size_t tasks_running_{0};
    size_t tasks_waiting_{0};
    size_t tasks_throttled_{0};
    size_t tasks_finished_{0};
  };

  Executor(Opts opts)
      : opts_(opts), impl_(ScalingThreadpool::getInstance().create(opts_)) {}

  const Opts& get_opts() const { return opts_; }

  void post(Func func);
  void post(Func func, int priority);
  void post(Func func, std::chrono::time_point<Clock> deadline,
            Func expireCallback = nullptr);

 private:
  const Opts opts_;

  Stats stats_;

  ExecutorImpl* impl_;

  Func pop();
};
}
