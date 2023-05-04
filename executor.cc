#include "executor.h"

#include <glog/logging.h>

#include <cmath>

namespace theta {

bool ExecutorStats::reserve_active() {
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

void ExecutorStats::unreserve_active() {
  active_.num.fetch_sub(1, std::memory_order::acq_rel);
}

void ExecutorStats::set_active_limit(uint32_t val) {
  active_.limit.store(val, std::memory_order::release);
}

std::pair<int, int> ExecutorStats::active_num_limit(
    std::memory_order mem_order) const {
  Active a{/*line_=*/active_.line.load(mem_order)};
  return {a.num.load(std::memory_order::relaxed),
          a.limit.load(std::memory_order::relaxed)};
}

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
  active_.num.fetch_sub(1, std::memory_order::acq_rel);
}

double ExecutorStats::ema_usage_proportion() const {
  return ema_usage_proportion_.load(std::memory_order::relaxed);
}
void ExecutorStats::update_ema_usage_proportion(struct rusage* begin_ru,
                                                struct timeval* begin_tv,
                                                struct rusage* end_ru,
                                                struct timeval* end_tv) {
  // TODO(lpe): This should set the total_limit_ and the running_limit_...
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
      expected, desired, std::memory_order::release,
      std::memory_order::relaxed));
}

std::string ExecutorStats::debug_string() const {
  std::string s{"ExecutorStats{"};
  auto [active_num, active_limit] = active_num_limit();
  s += "active.num=" + std::to_string(active_num);
  s += ", active.limit=" + std::to_string(active_limit);
  s += ", waiting_num=" + std::to_string(waiting_num());
  s += ", running_num=" + std::to_string(running_num());
  s += ", throttled_num=" + std::to_string(throttled_num());
  s += ", finished_num=" + std::to_string(finished_num());
  s += ", ema_usage_proportion=" + std::to_string(ema_usage_proportion());
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

void ExecutorImpl::refill_queues(Task** take_first) {
  std::unique_lock lock{mu_, std::defer_lock};

  if (take_first) {
    *take_first = nullptr;
  }

  // Queue more tasks to run
  while (stats()->reserve_active()) {
    std::unique_ptr<Task> task = pop();
    if (!task) {
      stats()->unreserve_active();
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

int ExecutorImpl::throttled_worker_limit() const {
  int conc = std::thread::hardware_concurrency();
  return std::min(8 * conc,
                  static_cast<int>(conc / stats()->ema_usage_proportion()));
}

Executor::Executor(ExecutorImpl* impl) : impl_(impl) {}

}  // namespace theta
