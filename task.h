#pragma once

#ifndef _GNU_SOURCE
// _GNU_SOURCE is required for getrusage.
static_assert(false);
#endif

#include <glog/logging.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <shared_mutex>

#include "queue.h"
#include "semaphore.h"

namespace theta {

class ExecutorImpl;
class Worker;

enum class NicePriority {
  kNone = -1,
  kThrottled = 1,
  kNormal = 2,
  kPrioritized = 3,
};

class ThrottleList;

// A task wraps a Func with information about scheduling.
//
// States:
// TODO(lpe): Update this comment.
//
//   0                      0
//   |                      |
//   v                      v
// +------------------+   +---------------------+   +----------------------+
// | queued(executor) |-->| queued(prioritized) |-->| running(prioritized) |--+
// +------------------+   +---------------------+   +----------------------+  |
//               |  |                 ^                        ^              |
//               |  |       0         |                        |              |
//               |  |       |         |                        |              |
//               |  |       v         v                        v              |
//               |  |     +---------------------+   +----------------------+  |
//               |  +---->| queued(throttled)   |-->| running(throttled)   |--+
//               |        +---------------------+   +----------------------+  |
//               |                    ^                        ^              |
//               |          0         |                        |              |
//               |          |         |                        |              |
//               |          v         v                        v              |
//               |        +---------------------+   +----------------------+  |
//               +------->| queued(normal)      |-->| running(normal)      |--|
//                        +---------------------+   +----------------------+  |
//                                                                            |
//                                                              +----------+  |
//                                                              | finished |<-+
//                                                              +----------+
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
class Task {
  friend class ExecutorImpl;
  friend class Worker;
  friend class ThrottleList;

 public:
  using Func = std::function<void()>;

  class Opts {
   public:
    Func func() const { return func_; }
    Opts& set_func(Func val) {
      func_ = val;
      return *this;
    }

    ExecutorImpl* executor() const { return executor_; }
    Opts& set_executor(ExecutorImpl* val) {
      executor_ = val;
      return *this;
    }

    NicePriority nice_priority() const { return nice_priority_; }
    Opts& set_nice_priority(NicePriority val) {
      nice_priority_ = val;
      return *this;
    }

   private:
    Func func_{nullptr};
    ExecutorImpl* executor_{nullptr};
    NicePriority nice_priority_{NicePriority::kNormal};
  };

  enum class State {
    kCreated = -1,
    kQueuedExecutor = 1,
    kQueuedThreadpool = 2,
    kRunning = 3,
    kThrottled = 4,
    kFinished = 5,
  };

  static bool is_running_state(Task::State state);

  static void run(ExecutorImpl* executor, std::unique_ptr<Task> task);

  Task(Opts opts) : opts_(opts) {}

  const Opts& opts() const { return opts_; }
  operator bool() const { return opts().func() != nullptr; }

  State state() const;
  State set_state(State state);

  Worker* worker() const { return worker_; }
  void set_worker(Worker* val) { worker_ = val; }

 private:
  Opts opts_;
  mutable std::mutex mutex_;
  rusage begin_ru_;
  timeval begin_tv_;
  State state_{State::kCreated};
  NicePriority nice_priority_{NicePriority::kNormal};
  Worker* worker_{nullptr};

  std::shared_ptr<Task> prev_{nullptr};
  std::shared_ptr<Task> next_{nullptr};
  ThrottleList* throttle_list_{nullptr};

  State state(const std::unique_lock<std::mutex>&) const;
  State set_state(State state, const std::unique_lock<std::mutex>&);
};

template <typename T>
concept SemaphoreType = requires(T t) {
  t.release();
  t.acquire;
  { t.try_acquire() } -> std::convertible_to<bool>;
};

template <typename SemaphoreType = Semaphore>
class TaskQueue {
 public:
  using Func = Task::Func;

  TaskQueue(size_t max_tasks = 512)
      : sem_(0), queue_(QueueOpts{}.set_max_size(max_tasks)) {}

  void shutdown() {
    shutdown_.store(true);
    sem_.release(std::numeric_limits<int32_t>::max() / 2);
  }

  bool is_shutting_down() const {
    return shutdown_.load(std::memory_order_acquire);
  }

  void push(std::unique_ptr<Task> task) {
    DCHECK(task);

    auto v = queue_.push_back(task.release());
    DCHECK(!v);
    sem_.release();
  }

  std::unique_ptr<Task> maybe_pop() {
    if (!sem_.try_acquire()) {
      return nullptr;
    }

    auto* v = queue_.pop_front();
    if (!v) {
      sem_.release(1);
      return nullptr;
    }

    return std::unique_ptr<Task>{v};
  }

  std::unique_ptr<Task> wait_pop() {
    while (true) {
      sem_.acquire();
      if (shutdown_.load(std::memory_order_acquire)) {
        return nullptr;
      }

      auto* v = queue_.pop_front();
      if (v) {
        return std::unique_ptr<Task>{v};
      }

      sem_.release(1);
    }
  }

  size_t size() const { return queue_.size(); }

 private:
  SemaphoreType sem_;

  Queue<Task> queue_;
  std::atomic<bool> shutdown_{false};
};

class ThrottleList {
 public:
  ThrottleList();

  void append(std::shared_ptr<Task> task);
  void remove(Task* task);

  bool throttle_one();
  bool unthrottle_one();

 private:
  const std::shared_ptr<Task> head_;
  const std::shared_ptr<Task> tail_;
  std::shared_ptr<Task> throttle_head_;
};

}  // namespace theta
