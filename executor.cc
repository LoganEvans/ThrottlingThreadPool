#include "executor.h"

#include <glog/logging.h>

namespace theta {

ExecutorStats::ExecutorStats(bool run_state_is_normal)
    : run_state_is_normal_(run_state_is_normal) {}

bool ExecutorStats::run_state_is_normal() const { return run_state_is_normal_; }

int ExecutorStats::running_num() const {
  return running_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::running_delta(int val) {
  running_num_.fetch_add(val, std::memory_order_release);
}

int ExecutorStats::waiting_num() const {
  return waiting_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::waiting_delta(int val) {
  waiting_num_.fetch_add(val, std::memory_order_release);
}

int ExecutorStats::throttled_num() const {
  return throttled_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::throttled_delta(int val) {
  throttled_num_.fetch_add(val, std::memory_order_release);
}

int ExecutorStats::finished_num() const {
  return finished_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::finished_delta(int val) {
  finished_num_.fetch_add(val, std::memory_order_release);
}

int ExecutorStats::running_limit() const {
  return running_limit_.load(std::memory_order_relaxed);
}
void ExecutorStats::set_running_limit(int val) {
  running_limit_.store(val, std::memory_order_relaxed);
}

int ExecutorStats::throttled_limit() const {
  return throttled_limit_.load(std::memory_order_relaxed);
}
void ExecutorStats::set_throttled_limit(int val) {
  throttled_limit_.store(val, std::memory_order_relaxed);
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

std::shared_ptr<Task> ExecutorImpl::maybe_execute_immediately(
    std::shared_ptr<Task> task) {
  if (stats()->running_num() < stats()->running_limit()) {
    stats()->running_delta(1);
    executing_.push(task);
    opts()
        .task_queues()
        ->queue(stats()->run_state_is_normal()
                    ? TaskQueues::NicePriority::kRunning
                    : TaskQueues::NicePriority::kPrioritized)
        ->push(std::move(task));
    return nullptr;
  } else if (stats()->throttled_num() < stats()->throttled_limit()) {
    stats()->throttled_delta(1);
    executing_.push(task);
    opts()
        .task_queues()
        ->queue(TaskQueues::NicePriority::kThrottled)
        ->push(std::move(task));
    return nullptr;
  }

  return task;
}

}  // namespace theta
