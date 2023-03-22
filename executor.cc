#include "executor.h"

#include <glog/logging.h>

namespace theta {

ExecutorStats::ExecutorStats(bool run_state_is_normal)
    : run_state_is_normal_(run_state_is_normal) {}

bool ExecutorStats::run_state_is_normal() const { return run_state_is_normal_; }

size_t ExecutorStats::num_running() const {
  return num_running_.load(std::memory_order_relaxed);
}
size_t ExecutorStats::num_waiting() const {
  return num_waiting_.load(std::memory_order_relaxed);
}
size_t ExecutorStats::num_throttled() const {
  return num_throttled_.load(std::memory_order_relaxed);
}
size_t ExecutorStats::num_finished() const {
  return num_finished_.load(std::memory_order_relaxed);
}

size_t ExecutorStats::limit_running() const {
  return limit_running_.load(std::memory_order_relaxed);
}
void ExecutorStats::set_limit_running(size_t val) {
  limit_running_.store(val, std::memory_order_relaxed);
}

size_t ExecutorStats::limit_throttled() const {
  return limit_throttled_.load(std::memory_order_relaxed);
}

Executor::Executor(Impl* impl) : impl_(impl) {}

bool Executor::Impl::maybe_run_immediately(Func func) {
  if (opts().maybe_run_immediately_callback()(opts().threadpool(), &stats_,
                                              func)) {
    return true;
  }
  return false;
}

}  // namespace theta
