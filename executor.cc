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

/*static*/
void ExecutorImpl::get_tv(timeval* tv) {
  auto tp = Clock::now();
  auto secs = time_point_cast<std::chrono::seconds>(tp);
  auto usecs = time_point_cast<std::chrono::microseconds>(tp) -
               time_point_cast<std::chrono::microseconds>(secs);
  tv->tv_sec = secs.time_since_epoch().count();
  tv->tv_usec = usecs.count();
}

void ExecutorImpl::refill_queues() {
  static std::mutex mu;
  std::lock_guard l{mu};

  int running_limit = stats()->running_limit();
  int running_num = stats()->running_num();

  // Throttle a running task
  for (; running_num > running_limit; running_num--) {
    auto task = executing_.maybe_pop_back();
    if (!task) {
      executing_.push(std::move(task));
      break;
    }
    task->set_state(Task::State::kRunningThrottled);
    throttled_.push_front(std::move(task));
  }

  auto normal_queue = opts().task_queues()->queue(NicePriority::kRunning);

  // Unthrottle a running task
  for (; running_num < running_limit; running_num++) {
    auto task = throttled_.maybe_pop();
    if (!task) {
      break;
    }
    {
      bool unthrottled = false;
      {
        std::lock_guard lock{task->mutex_};
        if (task->state(lock) == Task::State::kRunningThrottled) {
          task->set_state(Task::State::kRunningNormal, lock);
          unthrottled = true;
        }
      }

      if (!unthrottled) {
        normal_queue->push(task);
      }
    }
    executing_.push(std::move(task));
  }

  // Queue more tasks to run normally
  for (; running_num < running_limit; running_num++) {
    auto task = maybe_pop();
    if (!task) {
      return;
    }
    executing_.push(task);
    normal_queue->push(std::move(task));
  }

  // Queue more tasks to run throttled
  int throttled_limit = throttled_worker_limit();
  auto throttled_queue = opts().task_queues()->queue(NicePriority::kThrottled);
  for (int throttled_num = stats()->throttled_num();
       throttled_num < throttled_limit; throttled_num++) {
    auto task = maybe_pop();
    if (!task) {
      return;
    }
    throttled_.push(task);
    throttled_queue->push(std::move(task));
  }
}

int ExecutorImpl::throttled_worker_limit() const {
  int conc = std::thread::hardware_concurrency();
  return std::min(8 * conc,
                  static_cast<int>(conc / stats()->ema_usage_proportion()));
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

}  // namespace theta
