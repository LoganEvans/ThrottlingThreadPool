#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "gtest/gtest.h"
#include "threadpool.h"

namespace theta {

using namespace std::chrono_literals;

TEST(FIFOExecutor, ctor) {
  Executor executor = ScalingThreadpool::getInstance().create(
      Executor::Opts{}
          .set_priority_policy(PriorityPolicy::FIFO)
          .set_thread_weight(5)
          .set_worker_limit(2)
          .set_require_low_latency(true));

  EXPECT_EQ(executor.opts().priority_policy(), PriorityPolicy::FIFO);
  EXPECT_EQ(executor.opts().thread_weight(), 5);
  EXPECT_EQ(executor.opts().worker_limit(), 2);
  EXPECT_EQ(executor.opts().require_low_latency(), true);
}

TEST(FIFOExecutor, post) {
  std::condition_variable cv;
  std::mutex mu;

  Executor executor = ScalingThreadpool::getInstance().create(Executor::Opts{});

  std::unique_lock<std::mutex> lock{mu};
  auto now = Executor::Clock::now();
  std::atomic<bool> jobRan{false};

  executor.post([&]() {
    jobRan.store(true, std::memory_order_release);
    cv.notify_all();
  });

  cv.wait_until(lock, now + 1s,
                [&]() { return jobRan.load(std::memory_order_acquire); });

  EXPECT_TRUE(jobRan.load(std::memory_order_acquire));
}

TEST(FIFOExecutor, saturate) {
  static constexpr int kJobs = 1;

  std::condition_variable cv;
  std::mutex mu;

  Executor executor = ScalingThreadpool::getInstance().create(Executor::Opts{});

  std::unique_lock<std::mutex> lock{mu};
  std::mutex jobMutex;
  std::atomic<int> jobsRun{0};

  auto job = std::function<void()>([&]() {
    std::lock_guard l{jobMutex};
    int jobs = 1 + jobsRun.fetch_add(1, std::memory_order_acq_rel);
    if (jobs == kJobs) {
      lock.unlock();
      cv.notify_one();
    }
  });

  jobMutex.lock();
  for (int i = 0; i < kJobs; i++) {
    executor.post(job);
  }
  auto now = Executor::Clock::now();
  jobMutex.unlock();

  cv.wait_until(lock, now + 1s, [&]() {
    return jobsRun.load(std::memory_order_acquire) == kJobs;
  });

  EXPECT_EQ(jobsRun.load(std::memory_order_acquire), kJobs);
}

}  // namespace theta
