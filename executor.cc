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

bool ExecutorStats::running_num_is_at_limit() const {
  return running_num() >= running_limit();
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

  // Remove finished tasks from the front of the running_ and throttled_ queues.
  while (true) {
    auto optional_task = running_.pop_front();
    if (!optional_task) {
      break;
    }
    auto task = std::move(optional_task.value());

    if (task->state() != Task::State::kFinished) {
      running_.push_front(std::move(task));
      break;
    }
  }

  while (true) {
    auto optional_task = throttled_.pop_front();
    if (!optional_task) {
      break;
    }
    auto task = std::move(optional_task.value());

    if (task->state() != Task::State::kFinished) {
      throttled_.push_front(std::move(task));
      break;
    }
  }

  // Throttle a running task
  while (running_num > running_limit) {
    auto optional_task = running_.pop_back();
    if (!optional_task) {
      break;
    }
    auto task = std::move(optional_task.value());

    if (task->set_state(Task::State::kThrottled) == Task::State::kThrottled) {
      throttled_.push_back(std::move(task));
      running_num--;
    }
  }

  // Unthrottle a running task
  while (running_num < running_limit) {
    auto optional_task = throttled_.pop_front();
    if (!optional_task) {
      break;
    }
    auto task = std::move(optional_task.value());

    if (task->set_state(Task::State::kRunning) == Task::State::kRunning) {
      running_.push_back(std::move(task));
      running_num++;
    }
  }

  // Queue more tasks to run
  for (; running_num < running_limit; running_num++) {
    auto optional_task = maybe_pop();
    if (!optional_task) {
      return;
    }
    auto task = std::move(optional_task.value());

    task->set_state(Task::State::kQueuedThreadpool);
    opts().run_queue()->push(std::move(task));
  }
}

int ExecutorImpl::throttled_worker_limit() const {
  int conc = std::thread::hardware_concurrency();
  return std::min(8 * conc,
                  static_cast<int>(conc / stats()->ema_usage_proportion()));
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

}  // namespace theta
