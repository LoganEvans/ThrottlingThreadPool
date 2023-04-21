#pragma once

#include <atomic>
#include <semaphore>

namespace theta {

class Semaphore {
 public:
  void release(size_t n = 1) {
    Data d = Data{1 + d_.line.fetch_add(
                          Data{/*waiters=*/0, /*count=*/static_cast<int32_t>(n)}
                              .line.load(std::memory_order::relaxed),
                          std::memory_order::acq_rel)};

    while (d.waiters.load(std::memory_order::relaxed)) {
      int64_t expected = d.line.load(std::memory_order::relaxed);
      Data want{/*waiters=*/0,
                /*count=*/d.count.load(std::memory_order::relaxed)};
      if (d_.line.compare_exchange_weak(
              expected, want.line.load(std::memory_order::relaxed),
              std::memory_order::release, std::memory_order::relaxed)) {
        sem_.release(d.waiters.load(std::memory_order::relaxed));
        return;
      }
      d = Data{expected};
    }
  }

  void acquire() {
    while (!try_acquire()) {
      d_.waiters.fetch_add(1, std::memory_order::acq_rel);
      sem_.acquire();
    }
  }

  bool try_acquire() {
    int32_t c = d_.count.load(std::memory_order::relaxed);
    while (c > 0) {
      if (d_.count.compare_exchange_weak(c, c - 1, std::memory_order::release,
                                         std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  int32_t count() const { return d_.count.load(std::memory_order::acquire); }

 private:
  union Data {
    struct {
      std::atomic<int32_t> waiters;
      std::atomic<int32_t> count;
    };
    std::atomic<int64_t> line;

    Data(int64_t l) : line(l) {}
    Data() : Data(0) {}

    Data(int32_t waiters, int32_t count) : waiters(waiters), count(count) {}
    Data& operator=(const Data& other) {
      line.store(other.line.load(std::memory_order::relaxed),
                 std::memory_order::relaxed);
      return *this;
    }
  } d_;
  std::counting_semaphore<std::numeric_limits<int32_t>::max()> sem_{0};

  void wait() { sem_.acquire(); }

  void wake(size_t n) { sem_.release(n); }
};

}  // namespace theta
