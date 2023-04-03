#include "threadpool.h"

#include <array>
#include <thread>

using namespace std::chrono_literals;

namespace theta {

/*static*/
ScalingThreadpool::ConfigureOpts
ScalingThreadpool::ConfigureOpts::defaultOpts() {
  return ConfigureOpts{}
      .set_nice_cores(std::thread::hardware_concurrency() / 8)
      .set_thread_limit(8 * std::thread::hardware_concurrency())
      .set_throttle_interval(100ms);
}

/*static*/
ScalingThreadpool& ScalingThreadpool::getInstance() {
  static ScalingThreadpool instance;
  return instance;
}

void ScalingThreadpool::configure(
    const ScalingThreadpool::ConfigureOpts& opts) {
  shared_mutex_.lock();
  opts_ = opts;
  shared_mutex_.unlock();
}

Executor ScalingThreadpool::create(Executor::Opts opts) {
  opts.set_task_queues(&queues_);
  std::unique_ptr<ExecutorImpl> impl;
  if (opts.priority_policy() == PriorityPolicy::FIFO) {
    impl = std::unique_ptr<FIFOExecutorImpl>(
        new FIFOExecutorImpl(std::move(opts)));
  } else {
    throw NotImplemented();
  }

  auto* ptr = impl.get();

  shared_mutex_.lock();
  executors_.push_back(std::move(impl));
  shared_mutex_.unlock();

  Executor ex{ptr};
  return ex;
}

ScalingThreadpool::~ScalingThreadpool() {
  for (auto& worker : workers_) {
    worker->shutdown();
  }

  for (auto prio : std::to_array({NicePriority::kThrottled,
                                  NicePriority::kRunning,
                                  NicePriority::kPrioritized})) {
    queues_.queue(prio)->unblock_workers(workers_.size());
  }
}

ScalingThreadpool::ScalingThreadpool() {
  opts_ = ConfigureOpts::defaultOpts();

  // TODO(lpe): Throttled and limit CPUs!
  workers_.reserve(opts_.thread_limit());
  for (size_t i = 0; i < opts_.thread_limit(); i++) {
    workers_.push_back(
        std::make_unique<Worker>(&queues_, NicePriority::kRunning));
  }
}
}  // namespace theta
