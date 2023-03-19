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

// Tasks:
//
//               +-------------+
//      +------->| prioritized +---------+
//      |        +------------ +         |
//      |             ^                  |
//      |             |                  |
//      |             v                  v
// +--------+    +-------------+    +----------+
// | queued | -> | throttled   | -> | finished |
// +--------+    +-------------+    +----------+
//      |             ^                  ^
//      |             |                  |
//      |             v                  |
//      |        +-------------+         |
//      +------->|  running    |---------+
//               +-------------+
//
// The scaling logic controls how many tasks from each Executor may be in each
// of the three active states. A low-latency Executor will be able to have
// tasks in a prioritized state, but cannot have tasks in a running state.
// Conversely, a normal Executor can have tasks in a running state, but cannot
// have tasks in a prioritized state.
//
// The number of tasks from each Executor allowed in the running/prioritized
// states is at least equal to the thread_weight configuration for the
// Executor. However, the scaling logic may temporarily increase the limit if
// the tasks are not consuming their resources. This is intended to allow extra
// IO-bound threads to run with a normal nice value. If an excess number of
// threads are running and then start to consume an unfair share of the
// resources, extra tasks will be throttled with a nice value of 20.
//
// A task can be started in a throttled state if the system has an excess of
// resources but the Executor has reached its limit for tasks in a
// running/prioritized state.
//
// The ScalingThreadpool can be configured to only allow running/prioritized
// tasks on a subset of available cores. A throttled task may run on any core.
// When a throttled task is a candidate to be promoted to a running/prioritized
// task, if it is running on one of the quiet cores, the scaler may use various
// heuristics and leave it in a throttled state.
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
