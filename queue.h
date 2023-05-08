#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace theta {

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

class QueueOpts {
 public:
  size_t max_size() const { return max_size_; }
  QueueOpts& set_max_size(size_t val) {
    max_size_ = val;
    return *this;
  }

 private:
  size_t max_size_{hardware_destructive_interference_size};
};

template <typename T>
auto constexpr is_atomic = false;

template <typename T>
auto constexpr is_atomic<std::atomic<T>> = std::atomic<T>::is_always_lock_free;

template <typename T>
auto constexpr can_be_atomic = is_atomic<std::atomic<T>>;

template <typename T>
static constexpr bool memset0_to_bool() {
  T t;
  memset(&t, 0, sizeof(T));
  return static_cast<bool>(t);
}

template <typename T>
concept ZeroableAtomType = requires(T t) {
  can_be_atomic<T>;
  static_cast<bool>(T{}) == false;
  memset0_to_bool<T>() == false;
};

template <ZeroableAtomType T>
class Queue {
  class Idx {
   public:
    static uint32_t size(Idx head, Idx tail, size_t buffer_size) {
      uint32_t t = tail.idx_;
      uint32_t h = head.idx_;
      if (t < h) {
        t += buffer_size;
      }
      return t - h;
    }

    Idx() : idx_(0) {}
    Idx(uint32_t v) : idx_(v) {}
    Idx(const Idx& other) : idx_(other.idx_) {}
    Idx& operator=(const Idx& other) {
      idx_ = other.idx_;
      return *this;
    }

    Idx& operator++(int) {
      idx_++;
      return *this;
    }

    Idx& inc_mod(size_t mod) {
      DCHECK((mod & (mod - 1)) == 0);
      (*this)++;
      idx_ &= (mod - 1);
      return *this;
    }

    operator uint32_t() const {
      return mangle(idx_);
    }

   private:
    uint32_t idx_{0};

    static inline uint32_t mangle(uint32_t v) {
      return (v & 63) | ((v & 7) << 3) | ((v >> 3) & 7);
    }
  };

 public:
  static constexpr size_t next_pow_2(int v) {
    if ((v & (v - 1)) == 0) {
      return v;
    }
    int lg_v = 8 * sizeof(v) - __builtin_clz(v);
    return 1 << lg_v;
  }

  Queue(QueueOpts opts)
      : ht_(/*head=*/0, /*tail=*/0),
        buf_(std::min(64UL, next_pow_2(opts.max_size()))) {
    CHECK(capacity());
    CHECK((buf_.size() & (buf_.size() - 1)) == 0);
  }

  ~Queue() {
    while (true) {
      auto v = pop_front();
      if (!v) {
        break;
      }
    }
  }

  bool push_back(T val) { return push_back(val, nullptr); }

  bool push_back(T val, size_t* num_items) {
    DCHECK(val);
    uint64_t expected = ht_.line.load(std::memory_order::acquire);
    Idx head, tail;
    do {
      size_t s = size(expected, buf_.size());
      if (s == capacity()) {
        if (num_items) {
          *num_items = s;
        }
        return false;
      } else if (num_items) {
        *num_items = s + 1;
      }

      head = HeadTail{expected}.head;
      tail = HeadTail{expected}.tail;
      tail.inc_mod(buf_.size());
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail{head, tail}.line.load(std::memory_order::relaxed),
        std::memory_order::release, std::memory_order::relaxed));

    auto index = HeadTail{expected}.tail;

    // It is possible that a pop operation has claimed this index but hasn't
    // yet performed its read.
    while (true) {
      T expect_zero{};
      if (buf_[index].compare_exchange_weak(expect_zero, val,
                                            std::memory_order::release,
                                            std::memory_order::relaxed)) {
        break;
      }
    }

    return true;
  }

  T pop_front() {
    auto maybe_index = reserve_for_pop();
    if (!maybe_index.has_value()) {
      return T{};
    }
    auto index = maybe_index.value();

    T t{};
    // It's possible that a push operation has obtained this index but hasn't
    // yet written its value which will cause us to spin.
    do {
      t = buf_[index].exchange(t, std::memory_order::acq_rel);
    } while (!t);

    return t;
  }

  size_t size() const {
    return size(ht_.line.load(std::memory_order::acquire), buf_.size());
  }

  size_t capacity() const { return buf_.size() - 1; }

 private:
  // TODO(lpe): It's possible to make this structure naturally fall back to a
  // traditional threadqueue, thereby removing the size limit. This would
  // require 5 index values:
  // head: Index of the next value to pop
  // tail: Index where the next value should push
  // split: If active, a point that splits the queue in half. If this happens,
  //        the head index will always live in one half while the tail index
  //        will always live in the other.
  // fallback_tail: If in fallback mode, this index operates in the same half as
  // the
  //                head. It indicates where the next value popped from the
  //                fallback queue should be placed.
  // fallback_head: If in fallback mode, this indicates where the next value to
  //                be placed in the fallback queue is located.
  //
  // This system requires all 5 values to be read atomically, so the fast-queue
  // size will be limitted to 4096 values (12 bits). This leaves 4 bits, one of
  // which can indicate whether the fallback mode is active.
  //
  // While in fallback mode, creating a Flusher object will need to refill the
  // pop half of the queue. If a push or a pop operation reaches the fallback
  // head/tail, then that operation will need to block while it offloads values
  // into the fallback queue or pulls more values from the fallback queue.
  //
  // Moving from fast-mode to fallback-mode will involve taking the mutex in
  // the push path, declaring the split point, and then moving the elements in
  // the push half into the fallback-queue.
  //
  // Moving from fallback-mode to fast-mode will again involve taking the mutex,
  // but then the two halves of the fast-queue will need to be stitched together
  // by moving elements one half to the other half.
  //
  // It's not clear how thread-friendly the transition from fallback- to
  // fast-mode can be. At the worst, it will be possible to set a bit that flags
  // all operations to block.

  union HeadTail {
    struct {
      Idx head;
      Idx tail;
    };
    std::atomic<uint64_t> line;

    HeadTail(uint64_t line_) : line(line_) {}
    HeadTail(Idx head_, Idx tail_) : head(head_), tail(tail_) {}
  } ht_;
  static_assert(sizeof(HeadTail) == sizeof(HeadTail::line), "");

  std::vector<std::atomic<T>> buf_;

  static inline constexpr size_t size(uint64_t line, size_t buf_size) {
    Idx head = HeadTail(line).head;
    Idx tail = HeadTail(line).tail;
    return Idx::size(head, tail, buf_size);
  }

  std::optional<Idx> reserve_for_pop() {
    uint64_t expected;
    Idx head, tail;
    do {
      expected = ht_.line.load(std::memory_order::acquire);
      if (size(expected, buf_.size()) == 0) {
        return {};
      }

      head = HeadTail(expected).head;
      head.inc_mod(buf_.size());
      tail = HeadTail(expected).tail;
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail(head, tail).line.load(std::memory_order::relaxed),
        std::memory_order::release, std::memory_order::relaxed));

    return HeadTail(expected).head;
  }
};

}  // namespace theta
