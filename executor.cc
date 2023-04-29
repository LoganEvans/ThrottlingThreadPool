#include "executor.h"

#include <glog/logging.h>

#include <cmath>

namespace theta {

// TODO(lpe): Get rid of run_state_is_normal_.
ExecutorStats::ExecutorStats(bool run_state_is_normal)
    : run_state_is_normal_(run_state_is_normal) {}

bool ExecutorStats::run_state_is_normal() const { return run_state_is_normal_; }

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

int ExecutorStats::running_limit(std::memory_order mem_order) const {
  return running_limit_.load(mem_order);
}
void ExecutorStats::set_running_limit(int val) {
  running_limit_.store(val, std::memory_order::relaxed);
}

int ExecutorStats::total_limit(std::memory_order mem_order) const {
  return total_limit_.load(mem_order);
}
void ExecutorStats::set_total_limit(int val) {
  total_limit_.store(val, std::memory_order::relaxed);
}

double ExecutorStats::ema_usage_proportion() const {
  return ema_usage_proportion_.load(std::memory_order::relaxed);
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
      expected, desired, std::memory_order::release,
      std::memory_order::relaxed));
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
  std::unique_lock lock{mu_, std::defer_lock};
  throttle_list_.set_running_limit(
      stats()->running_limit(std::memory_order::acquire), lock);

  // Queue more tasks to run
  // TODO(lpe): This has a potential for over-queueing tasks because there's a
  // race condition between checking how many tasks are queued and queuing
  // another one, which means that all but one worker could add a task that
  // shouldn't be added yet.
  while (throttle_list_.total(std::memory_order::acquire) <
         stats()->total_limit(std::memory_order::acquire)) {
    auto task = pop();
    if (!task) {
      return;
    }

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
