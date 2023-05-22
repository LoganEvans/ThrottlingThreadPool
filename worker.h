#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <thread>

#include "task.h"

namespace theta {

class ThrottlingThreadpool;
class Task;

class Worker {
 public:
  Worker(TaskQueue<>* run_queue);
  ~Worker();

  void shutdown();

  NicePriority nice_priority() const;
  void set_nice_priority(NicePriority priority);
  pthread_t get_pthread();

 private:
  TaskQueue<>* run_queue_;
  std::mutex priority_mutex_;

  union Priority {
    Priority(NicePriority actual_, NicePriority postponed_) : actual(actual_), postponed(postponed_) {}

    Priority() : Priority(NicePriority::kNormal, NicePriority::kNormal) {}

    Priority(uint64_t l) : line(l) {}

    Priority(const Priority& other,
             std::memory_order mem_order = std::memory_order::relaxed)
        : Priority(other.line.load(mem_order)) {}

    Priority& operator=(const Priority& other) {
      line.store(other.line.load(std::memory_order::relaxed),
                 std::memory_order::relaxed);
      return *this;
    }

    Ticket postpone(NicePriority prio) {
      NicePriority other_prio = (prio == NicePriority::kNormal)
                                    ? NicePriority::kThrottled
                                    : NicePriority::kNormal;
      Priority expected{/*actual_=*/other_prio,
                        /*postponed_=*/other_prio};
      const Priority want{/*actual=*/other_prio, /*postponed=*/prio};
      const uint64_t want_line = want.line(std::memory_order::relaxed);
      uint64_t expected_line = expected.line.load(std::memory_order::relaxed);
      do {
        Priority observed{expected_line};
        if (observed.actual == prio) {
          // Some other thread already set the priority to what we want.
          return false;
        }
        if (observed.postponed == prio) {
          // Some other thread already postponed the priority change.
          return false;
        }
      } while (line.compare_exchange_weak(expected_line, want_line,
                                          std::memory_order::release,
                                          std::memory_order::relaxed));
      return true;
    }

    bool change_is_postponed() const {
      NicePriority prio{line.load(std::memory_order::acquire)};
      if (prio.actual == prio.postponed) {
        return false;
      }
      return true;
    }

    struct Priority {
      uint64_t generation;
      NicePriority actual{NicePriority::kNormal};
      NicePriority postponed{NicePriority::kNormal};
    };
    std::atomic<uint64_t> line;
  } priority_;
  static_assert(sizeof(Priority) == sizeof(Priority::line), "");

  std::atomic<NicePriority> priority_{NicePriority::kNormal};
  std::thread thread_;

  void run_loop();
  void maybe_update_priority();
};

}  // namespace theta
