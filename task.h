#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <semaphore>
#include <shared_mutex>

namespace theta {

class ExecutorImpl;
class Worker;

enum class NicePriority {
  kThrottled,
  kRunning,
  kPrioritized,
};

// A task wraps a Func with information about scheduling.
//
// States:
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
  friend class Worker;

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

    Worker* worker() const { return worker_; }
    Opts& set_worker(Worker* val) {
      worker_ = val;
      return *this;
    }

   private:
    Func func_{nullptr};
    ExecutorImpl* executor_{nullptr};
    Worker* worker_{nullptr};
  };

  enum class State {
    kCreated = 0,
    kQueuedExecutor = 1,
    kQueuedPrioritized = 2,
    kQueuedThrottled = 3,
    kQueuedNormal = 4,
    kRunningPrioritized = 5,
    kRunningThrottled = 6,
    kRunningNormal = 7,
    kFinished = 8,
  };

  Task(Opts opts) : opts_(opts) {}

  const Opts& opts() const { return opts_; }
  operator bool() const { return opts().func() != nullptr; }

  State state() const;
  void set_state(State state);

  NicePriority nice_priority() const;
  void set_nice_priority(NicePriority priority);

 private:
  Opts opts_;
  mutable std::mutex mutex_;
  State state_{State::kCreated};
  NicePriority nice_priority_{NicePriority::kRunning};

  void set_state(State state, const std::lock_guard<std::mutex>&);
  void set_nice_priority(NicePriority priority,
                         const std::lock_guard<std::mutex>&);
  void run();
};

class TaskQueue {
 public:
  using Func = Task::Func;

  void push(std::shared_ptr<Task> task);
  std::shared_ptr<Task> pop();
  std::shared_ptr<Task> pop_blocking();

  // TODO(lpe): It would be nice to have a remove function that allowed for a
  // Task to removed from the center of a queue. The current workaround is to
  // allow a Task to be in a zombie state.
  void reap_finished();

  void unblock_workers(size_t n);

 private:
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> sem_{0};

  // TODO(lpe): This needs to be lock-free. This mutex is for quick development
  // only.
  std::shared_mutex shared_mutex_;
  std::deque<std::shared_ptr<Task>> queue_;

  std::shared_ptr<Task> pop_impl();
  void reap_finished(const std::unique_lock<std::shared_mutex>&);
};

class TaskQueues {
 public:
  TaskQueue* queue(NicePriority priority);

  void push(NicePriority priority, std::shared_ptr<Task> task);
  std::shared_ptr<Task> pop_blocking(NicePriority priority);

 private:
  TaskQueue throttled_queue_;
  TaskQueue running_queue_;
  TaskQueue prioritized_queue_;
};

}  // namespace theta
