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

enum class NicePriority : uint32_t {
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

  static void run(std::unique_ptr<Task> task);

  Task(Opts opts);

  const Opts& opts() const { return opts_; }
  operator bool() const { return opts().func() != nullptr; }

  State state(std::memory_order mem_order = std::memory_order::acquire) const;
  State set_state(State state);

  Worker* worker(
      std::memory_order mem_order = std::memory_order::acquire) const;
  void set_worker(Worker* val,
                  std::memory_order mem_order = std::memory_order::release);

 private:
  // TODO(lpe): Make opts_, begin_ru_, and begin_tv_ const.
  const Opts opts_;
  rusage begin_ru_;
  rusage end_ru_;
  timeval begin_tv_;
  timeval end_tv_;

  std::atomic<State> state_{State::kCreated};
  std::atomic<Worker*> worker_{nullptr};

  ThrottleList* throttle_list_{nullptr};
  // These variables are only read while holding throttle_list_->mtx_.
  Task* prev_{nullptr};
  Task* next_{nullptr};
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
    if (!shutdown_.exchange(true, std::memory_order::acq_rel)) {
      sem_.release(std::numeric_limits<int32_t>::max() / 2);
    }
  }

  bool is_shutting_down() const {
    return shutdown_.load(std::memory_order_acquire);
  }

  void push(std::unique_ptr<Task> task) {
    DCHECK(task);

    bool success = queue_.push_back(task.release());
    DCHECK(success);
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
      semaphoreAcquireKludge(sem_);
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

  MPSCQueue<Task*> queue_;
  std::atomic<bool> shutdown_{false};
};

class ThrottleList {
 public:
  ThrottleList(size_t modification_queue_size);

  void append(Task* task);
  void remove(Task* task);
  uint32_t running_limit(
      std::memory_order mem_order = std::memory_order::relaxed) const;
  void set_running_limit(size_t running_limit);

  uint32_t total(
      std::memory_order mem_order = std::memory_order::relaxed) const {
    return Count{count_, mem_order}.total();
  }

 private:
  struct Modification {
    enum class Op : uintptr_t {
      kAppend = 1,
      kRemove = 2,
    };

    Modification() : data_(0) {}

    Modification(Op op, Task* task)
        : data_(static_cast<uintptr_t>(op) |
                reinterpret_cast<uintptr_t>(task)) {}

    Op op() const { return static_cast<Op>(data_ & 0x3); }

    Task* task() const { return reinterpret_cast<Task*>(data_ & ~0x3); }

    operator bool() const { return static_cast<bool>(data_); }

   private:
    uintptr_t data_;
  };

  Task head_real_{Task::Opts{}};
  Task tail_real_{Task::Opts{}};
  Task* const head_{&head_real_};
  Task* const tail_{&tail_real_};
  Task* throttle_head_{tail_};

  struct Count {
    static constexpr int kFieldBits = 20;
    static constexpr uint64_t kFieldMask = (1ULL << kFieldBits) - 1;

    static constexpr int kRunningOffset = 0;
    static constexpr uint64_t kRunningMask = kFieldMask;

    static constexpr int kRunningLimitOffset = kFieldBits + 1;
    static constexpr uint64_t kRunningLimitMask = kFieldMask
                                                  << kRunningLimitOffset;

    static constexpr int kTotalOffset = kRunningLimitOffset + kFieldBits + 1;
    static constexpr uint64_t kTotalMask = kFieldMask << kTotalOffset;

    static constexpr uint64_t kValidMask =
        kRunningMask | kRunningLimitMask | kTotalMask;

    Count(uint64_t running, uint64_t running_limit, uint64_t total)
        : line_(running | (running_limit << kRunningLimitOffset) |
                (total << kTotalOffset)) {}

    Count(uint64_t line) : line_(line) {}

    Count() : Count(/*running=*/0, /*running_limit=*/1, /*total=*/0) {}

    Count(const Count& other,
          std::memory_order mem_order = std::memory_order::relaxed)
        : line_(other.line_.load(mem_order)) {}

    uint64_t line(
        std::memory_order mem_order = std::memory_order::relaxed) const {
      return line_.load(mem_order);
    }

    uint32_t running(
        std::memory_order mem_order = std::memory_order::relaxed) const {
      return line(mem_order) & kFieldMask;
    }
    void set_running(uint64_t val,
                     std::memory_order mem_order = std::memory_order::relaxed) {
      line_.store((line() & ~kRunningMask) | (val << kRunningOffset),
                  mem_order);
    }
    uint32_t running_delta(int delta) {
      if (delta > 0) {
        return delta + line_.fetch_add(delta << kRunningOffset,
                                       std::memory_order::acq_rel);
      } else {
        return (-delta) + line_.fetch_sub((-delta) << kRunningOffset,
                                          std::memory_order::acq_rel);
      }
    }

    uint32_t running_limit(
        std::memory_order mem_order = std::memory_order::relaxed) const {
      return (line(mem_order) >> kRunningLimitOffset) & kFieldMask;
    }
    void set_running_limit(uint64_t val, std::memory_order mem_order =
                                             std::memory_order::relaxed) {
      line_.store((line() & ~kRunningLimitMask) | (val << kRunningLimitOffset),
                  mem_order);
    }

    uint32_t total(
        std::memory_order mem_order = std::memory_order::relaxed) const {
      return (line(mem_order) >> kTotalOffset) & kFieldMask;
    }
    void set_total(uint64_t val,
                   std::memory_order mem_order = std::memory_order::relaxed) {
      line_.store((line() & ~kTotalMask) | (val << kTotalOffset), mem_order);
    }
    uint32_t total_delta(int delta) {
      if (delta > 0) {
        return delta +
               line_.fetch_add(static_cast<uint64_t>(delta) << kTotalOffset,
                               std::memory_order::acq_rel);
      } else {
        return -delta +
               line_.fetch_sub(static_cast<uint64_t>(-delta) << kTotalOffset,
                               std::memory_order::acq_rel);
      }
    }

    std::atomic<uint64_t> line_{0};
  } count_;

  MPSCQueue<Modification> modification_queue_;
  std::mutex mtx_;

  void flush_modifications(bool wait_for_mtx = false);
  void adjust_throttle_head(std::unique_lock<std::mutex>&);
};

}  // namespace theta
