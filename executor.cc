#include "executor.h"

#include <glog/logging.h>

#include <cmath>

namespace theta {

int ExecutorStats::running_num(std::memory_order mem_order) const {
  return running_num_.load(mem_order);
}
void ExecutorStats::running_delta(int val) {
  running_num_.fetch_add(val, std::memory_order::acq_rel);
}

int ExecutorStats::waiting_num(std::memory_order mem_order) const {
  return waiting_num_.load(mem_order);
}
void ExecutorStats::waiting_delta(int val) {
  waiting_num_.fetch_add(val, std::memory_order::acq_rel);
}

int ExecutorStats::throttled_num(std::memory_order mem_order) const {
  return throttled_num_.load(mem_order);
}
void ExecutorStats::throttled_delta(int val) {
  throttled_num_.fetch_add(val, std::memory_order::acq_rel);
}

int ExecutorStats::finished_num(std::memory_order mem_order) const {
  return finished_num_.load(mem_order);
}
void ExecutorStats::finished_delta(int val) {
  finished_num_.fetch_add(val, std::memory_order::acq_rel);
}

double ExecutorStats::ema_usage_proportion(std::memory_order mem_order) const {
  return ema_usage_proportion_.load(mem_order);
}

double ExecutorStats::ema_nivcsw_per_task(std::memory_order mem_order) const {
  return ema_nivcsw_per_task_.load(mem_order);
}

void ExecutorStats::update_ema(struct rusage* begin_ru,
                               struct timeval* begin_tv, struct rusage* end_ru,
                               struct timeval* end_tv) {
  static constexpr double kTau = 1.0;  // I.e., average events per second.

  auto tvInterval = [](struct timeval* start, struct timeval* end) {
    double sec = ((end->tv_sec * 1000000 + end->tv_usec) -
                  (start->tv_sec * 1000000 + start->tv_usec)) /
                 1e6;
    return sec / kTau;
  };

  {
    double interval = tvInterval(begin_tv, end_tv);
    double alpha = 1.0 - exp(-interval / kTau);
    double usage = tvInterval(&begin_ru->ru_utime, &end_ru->ru_utime);
    double proportion = interval ? usage / interval : 0.0;

    double expected = ema_usage_proportion(std::memory_order::relaxed);
    double desired;
    do {
      desired = expected + alpha * (proportion - expected);
    } while (!ema_usage_proportion_.compare_exchange_weak(
        expected, desired, std::memory_order::release,
        std::memory_order::relaxed));
  }

  do {
    timeval last_update_tv = ema_last_update_.load(std::memory_order::acquire);
    double interval = tvInterval(&last_update_tv, end_tv);
    ema_last_update_.compare_exchange_strong(last_update_tv, *end_tv,
                                             std::memory_order::release,
                                             std::memory_order::relaxed);
    if (last_update_tv.tv_sec == 0) {
      break;
    }

    double alpha = 1.0 - exp(-interval / kTau);
    int64_t delta_nivcsw = end_ru->ru_nivcsw - begin_ru->ru_nivcsw;

    double expected = ema_nivcsw_per_task(std::memory_order::relaxed);
    double desired;
    do {
      desired = expected + alpha * (delta_nivcsw - expected);
    } while (!ema_nivcsw_per_task_.compare_exchange_weak(expected, desired,
                                                std::memory_order::release,
                                                std::memory_order::relaxed));
  } while (0);
}

std::string ExecutorStats::debug_string() const {
  std::string s{"ExecutorStats{"};
  s += "waiting=" + std::to_string(waiting_num());
  s += ", running=" + std::to_string(running_num());
  s += ", throttled=" + std::to_string(throttled_num());
  s += ", finished=" + std::to_string(finished_num());
  s += ", ema_usage_proportion=" + std::to_string(ema_usage_proportion());
  s += ", ema_nivcsw_per_task=" + std::to_string(ema_nivcsw_per_task());
  s += "}";
  return s;
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

std::string ExecutorImpl::debug_string() const {
  std::string s{"ExecutorImpl{"};
  auto [active_num, active_limit] = active_num_limit();
  s += "active=" + std::to_string(active_num);
  s += ", limit=" + std::to_string(active_limit);
  s += ", " + stats_.debug_string();
  s += "}";
  return s;
}

void ExecutorImpl::refill_queues(Task** take_first) {
  if (take_first) {
    *take_first = nullptr;
  }

  refresh_limits();

  // Queue more tasks to run
  while (reserve_active()) {
    std::unique_ptr<Task> task = pop();
    if (!task) {
      unreserve_active();
      return;
    }

    if (take_first && !*take_first) {
      *take_first = task.release();
    } else {
      task->set_state(Task::State::kQueuedThreadpool);
      opts().run_queue()->push(std::move(task));
    }
  }
}

bool ExecutorImpl::reserve_active() {
  uint64_t expected = active_.line.load(std::memory_order::acquire);
  Active desired{0};
  do {
    desired = Active{expected};
    desired.num.fetch_add(1, std::memory_order::relaxed);
    if (desired.num.load(std::memory_order::relaxed) >
        desired.limit.load(std::memory_order::relaxed)) {
      return false;
    }
  } while (!active_.line.compare_exchange_weak(
      expected, desired.line.load(std::memory_order::relaxed),
      std::memory_order::release, std::memory_order::relaxed));

  return true;
}

void ExecutorImpl::unreserve_active() {
  active_.num.fetch_sub(1, std::memory_order::acq_rel);
}

void ExecutorImpl::set_active_limit(uint32_t val) {
  active_.limit.store(val, std::memory_order::release);
}

std::pair<int, int> ExecutorImpl::active_num_limit(
    std::memory_order mem_order) const {
  Active a{/*line_=*/active_.line.load(mem_order)};
  return {a.num.load(std::memory_order::relaxed),
          a.limit.load(std::memory_order::relaxed)};
}

void ExecutorImpl::refresh_limits() {
  int concurrency = std::thread::hardware_concurrency();
  size_t active_limit =
      concurrency / stats_.ema_usage_proportion(std::memory_order::acquire);
  set_active_limit(std::min(active_limit, opts_.worker_limit()));

  double ema_nivcsw_per_task =
      stats_.ema_nivcsw_per_task(std::memory_order::acquire);
  if (ema_nivcsw_per_task > 0.0) {
    double tasks_per_interrupt = 1.0 / ema_nivcsw_per_task;
    throttle_list_.set_running_limit(std::max(
        static_cast<size_t>(tasks_per_interrupt), opts_.thread_weight()));
  }
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

}  // namespace theta
