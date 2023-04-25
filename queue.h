#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

namespace theta {

class QueueOpts {
 public:
  size_t max_size() const { return max_size_; }
  QueueOpts& set_max_size(size_t val) {
    max_size_ = val;
    return *this;
  }

 private:
  size_t max_size_{512};
};

template <typename T>
class Queue {
  static constexpr size_t next_pow_2(int v) {
    int lg_v = 8 * sizeof(v) - __builtin_clz(v);
    return 1 << lg_v;
  }

 public:
  Queue(QueueOpts opts)
      : ht_(/*head=*/0, /*tail=*/0), buf_(next_pow_2(opts.max_size())) {}

  ~Queue() {
    while (true) {
      auto v = pop_front();
      if (!v) {
        break;
      }
    }
  }

  T* push_back(T* val) {
    while (val) {
      uint64_t expected = ht_.line.load(std::memory_order_relaxed);
      uint32_t head, tail;
      do {
        head = HeadTail::to_head(expected);
        tail = HeadTail::to_tail(expected);
        if (tail == 0) {
          tail = buf_.size() - 1;
        } else {
          tail--;
        }

        if (tail == head) {
          // Attempt to push on a full queue. Need to wait until something is
          // popped.
          fprintf(stderr, "push_back full\n");
          return val;
        }
      } while (!ht_.line.compare_exchange_weak(
          expected, HeadTail::to_line(/*head=*/head, /*tail=*/tail),
          std::memory_order::release, std::memory_order::relaxed));

      // In some cases, e.g. push -> pop -> push, two threads will attempt to
      // push to the same index. If this happens, push the old value back into
      // the queue.
      uint32_t index = HeadTail::to_tail(expected);
      val = buf_[index].exchange(val, std::memory_order::acq_rel);
    }

    return nullptr;
  }

  T* push_front(T* val) {
    while (val) {
      uint64_t expected = ht_.line.load(std::memory_order_relaxed);
      uint32_t head, tail;
      do {
        head = HeadTail::to_head(expected);
        tail = HeadTail::to_tail(expected);
        if (head == buf_.size() - 1) {
          head = 0;
        } else {
          head++;
        }

        if (head == tail) {
          // Attempt to push on a full queue. Need to wait until something is
          // popped.
          return val;
        }
      } while (!ht_.line.compare_exchange_weak(
          expected, HeadTail::to_line(/*head=*/head, /*tail=*/tail),
          std::memory_order::release, std::memory_order::relaxed));

      // In some cases, e.g. push -> pop -> push, two threads will attempt to
      // push to the same index. If this happens, push the old value back into
      // the queue.
      int32_t index = HeadTail::to_head(expected);
      val = buf_[index].exchange(val, std::memory_order::acq_rel);
    }

    return {};
  }

  T* pop_front() {
    uint64_t expected = ht_.line.load(std::memory_order_relaxed);
    uint32_t head, tail;
    do {
      head = HeadTail::to_head(expected);
      tail = HeadTail::to_tail(expected);

      if (head == tail) {
        // Attempt to pop from an empty queue.
        return {};
      }

      if (head == 0) {
        head = buf_.size() - 1;
      } else {
        head--;
      }
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail::to_line(head, tail), std::memory_order::release,
        std::memory_order::relaxed));

    uint32_t index = HeadTail::to_head(expected);
    T* t{nullptr};
    // It's possible that a push operation has obtained this index but hasn't
    // yet written its value which will cause us to spin.
    do {
      t = buf_[index].exchange(nullptr, std::memory_order::acq_rel);
    } while (!t);

    return t;
  }

  T* pop_back() {
    uint64_t expected = ht_.line.load(std::memory_order_relaxed);
    uint32_t head, tail;
    do {
      head = HeadTail::to_head(expected);
      tail = HeadTail::to_tail(expected);

      if (head == tail) {
        // Attempt to pop from an empty queue.
        return {};
      }

      if (tail == buf_.size() - 1) {
        tail = 0;
      } else {
        tail++;
      }
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail::to_line(head, tail), std::memory_order::release,
        std::memory_order::relaxed));

    uint32_t index = HeadTail::to_tail(expected);
    T* t{nullptr};
    // It's possible that a push operation has obtained this index but hasn't
    // yet written its value which will cause us to spin.
    do {
      t = buf_[index].exchange(nullptr, std::memory_order::acq_rel);
    } while (!t);

    return t;
  }

  size_t size() const {
    uint64_t line = ht_.line.load(std::memory_order_acquire);
    uint32_t head = HeadTail::to_head(line);
    uint32_t tail = HeadTail::to_tail(line);
    if (head < tail) {
      head += buf_.size();
    }
    return head - tail;
  }

  size_t capacity() const { return buf_.size() - 1; }

 private:
  union HeadTail {
    static constexpr uint64_t to_line(uint32_t head, uint32_t tail) {
      return (static_cast<uint64_t>(head) << 32) | tail;
    }

    static constexpr uint32_t to_head(uint64_t line) { return line >> 32; }

    static constexpr uint32_t to_tail(uint64_t line) {
      return static_cast<uint32_t>(line);
    }

    struct {
      std::atomic<uint32_t> head;
      std::atomic<uint32_t> tail;
    };
    std::atomic<uint64_t> line;

    HeadTail(uint32_t head, uint32_t tail) : line(to_line(head, tail)) {}
  } ht_;

  std::vector<std::atomic<T*>> buf_;
};

}  // namespace theta
