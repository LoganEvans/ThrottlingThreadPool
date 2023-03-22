#pragma once

#include <functional>
#include <queue>
#include <semaphore>
#include <shared_mutex>

namespace theta {

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
//               |  |       v         |                        v              |
//               |  |     +---------------------+   +----------------------+  |
//               |  +---->| queued(throttled)   |-->| running(throttled)   |--+
//               |        +---------------------+   +----------------------+  |
//               |                    |                        ^              |
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
 public:
  using Func = std::function<void()>;
};

class TaskQueue {
 public:
  using Func = Task::Func;

  void push(Task::Func func);
  Task::Func pop();
  Task::Func pop_blocking();

  void unblock_workers(size_t n);

 private:
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> sem_{0};

  // TODO(lpe): This needs to be lock-free. This mutex is for quick development
  // only.
  std::shared_mutex shared_mutex_;
  std::queue<Task::Func> queue_;
};

class TaskQueues {
 public:
  enum class NicePriority {
    kThrottled,
    kRunning,
    kPrioritized,
  };

  TaskQueue* queue(NicePriority priority);

  void push(NicePriority priority, Task::Func func);
  Task::Func pop_blocking(NicePriority priority);

 private:
  TaskQueue throttled_queue_;
  TaskQueue running_queue_;
  TaskQueue prioritized_queue_;
};

}  // namespace theta
