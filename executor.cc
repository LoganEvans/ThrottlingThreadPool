#include "executor.h"

#include <glog/logging.h>

#include <cmath>

namespace theta {

ExecutorStats::ExecutorStats(bool run_state_is_normal)
    : run_state_is_normal_(run_state_is_normal) {}

bool ExecutorStats::run_state_is_normal() const { return run_state_is_normal_; }

int ExecutorStats::running_num() const {
  return running_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::running_delta(int val) {
  running_num_.fetch_add(val, std::memory_order_acq_rel);
}

int ExecutorStats::waiting_num() const {
  return waiting_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::waiting_delta(int val) {
  waiting_num_.fetch_add(val, std::memory_order_acq_rel);
}

int ExecutorStats::throttled_num() const {
  return throttled_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::throttled_delta(int val) {
  throttled_num_.fetch_add(val, std::memory_order_acq_rel);
}

int ExecutorStats::finished_num() const {
  return finished_num_.load(std::memory_order_relaxed);
}
void ExecutorStats::finished_delta(int val) {
  finished_num_.fetch_add(val, std::memory_order_acq_rel);
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

double ExecutorStats::ema_usage_proportion() const {
  return ema_usage_proportion_.load(std::memory_order_relaxed);
}
void ExecutorStats::update_ema_usage_proportion(struct rusage* begin_ru,
                                                struct timeval* begin_tv,
                                                struct rusage* end_ru,
                                                struct timeval* end_tv) {
  auto tvInterval = [](struct timeval* start, struct timeval* end) {
    double sec =
        (end->tv_sec - start->tv_sec) + (end->tv_usec - start->tv_usec) / 1e6;
    return sec / tau_;
  };

  double interval = tvInterval(begin_tv, end_tv);
  double usage = tvInterval(&begin_ru->ru_utime, &end_ru->ru_utime);
  double proportion = usage / interval;

  double expected = ema_usage_proportion();
  double desired;

  do {
    double alpha = 1.0 - exp(-tvInterval(begin_tv, end_tv) / tau_);
    double s_old = ema_usage_proportion();
    desired = s_old + alpha * (proportion - s_old);
  } while (!ema_usage_proportion_.compare_exchange_weak(
      expected, desired, std::memory_order_release, std::memory_order_relaxed));
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

/*static*/
void ExecutorImpl::get_tv(timeval* tv) {
  auto tp = Clock::now();
  auto secs = time_point_cast<std::chrono::seconds>(tp);
  auto usecs = time_point_cast<std::chrono::microseconds>(tp) -
               time_point_cast<std::chrono::microseconds>(secs);
  tv->tv_sec = secs.time_since_epoch().count();
  tv->tv_usec = usecs.count();
}

std::shared_ptr<Task> ExecutorImpl::maybe_execute_immediately(
    std::shared_ptr<Task> task) {
  if (stats()->running_num() < stats()->running_limit()) {
    executing_.push(task);
    opts()
        .task_queues()
        ->queue(stats()->run_state_is_normal() ? NicePriority::kRunning
                                               : NicePriority::kPrioritized)
        ->push(std::move(task));
    return nullptr;
  } else if (stats()->throttled_num() < stats()->throttled_limit()) {
    executing_.push(task);
    opts()
        .task_queues()
        ->queue(NicePriority::kThrottled)
        ->push(std::move(task));
    return nullptr;
  }

  return task;
}

}  // namespace theta
