#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
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
concept AtomType = requires(T t) {
  std::is_trivially_constructible<T>::value;
  can_be_atomic<T>;
  sizeof(T) <= 8;
};

template <typename T>
class alignas(hardware_destructive_interference_size) ExclusiveCacheLine {
  static_assert(sizeof(T) <= hardware_destructive_interference_size, "");

 public:
  template <typename... Args>
  ExclusiveCacheLine(Args... args) : t_(std::forward<Args>(args)...) {}

  T& get() { return t_; }
  const T& get() const { return t_; }

 private:
  T t_;
};

template <AtomType T, size_t kBufferSize = 128>
class Queue {
  struct Tag {
    static_assert((kBufferSize & (kBufferSize - 1)) == 0, "");

    static constexpr uint64_t kIncrement =
        1 + hardware_destructive_interference_size / 16;
    static constexpr uint64_t kBufferWrapDelta = kBufferSize * kIncrement;
    static constexpr uint64_t kBufferSizeMask = kBufferSize - 1;

    uint64_t value;

    Tag() : value(0) {}

    Tag& operator++() {
      value += kIncrement;
      return *this;
    }

    Tag& operator++(int) {
      Tag orig{*this};
      value += kIncrement;
      return orig;
    }

    auto operator<=>(const Tag&) const = default;

    Tag prev_paired_tag() const {
      return Tag{.value = value - kBufferWrapDelta};
    }

    bool is_paired(Tag observed_tag) const {
      if (is_consumer()) {
        return (value ^ observed_tag.value) == (1ULL << 63);
      } else {
        return observed_tag.value == ((value - kBufferWrapDelta) | (1ULL << 63));
      }
    }

    bool is_producer() const {
      return value >> 63 == 0;
    }

    void mark_as_producer() {
      value &= ~(1ULL << 63);
    }

    bool is_consumer() const {
      return value >> 63 == 1;
    }

    void mark_as_consumer() {
      value |= (1ULL << 63);
    }

    Tag next_tag() const {
      return value + kIncrement;
    }

    int to_index() const {
      return value & kBufferSizeMask;
    }
  };
  static_assert(sizeof(Tag) == sizeof(uint64_t), "");

  union HeadTail {
    struct {
      Tag head_tag;
      Tag tail_tag;
    };
    std::atomic<__int128> line;

    HeadTail(Tag head, Tag tail) : head_tag(head), tail_tag(tail) {}
    HeadTail(__int128 from_line) : line(from_line) {}
    HeadTail() : HeadTail(/*line=*/0) {}
  };
  static_assert(sizeof(HeadTail) == sizeof(HeadTail::line), "");

  union Data {
    struct {
      T value;
      Tag tag;
    };
    struct {
      std::atomic<T> value_atomic;
      std::atomic<Tag> tag_atomic;
    };
    std::atomic<__int128> line;

    Data(T value_, Tag tag_) : value(value_), tag(tag_) {}
    Data(__int128 line) : line(line) {}
    Data() : Data(/*line=*/0) {}
  };
  static_assert(sizeof(Data) == sizeof(Data::line), "");
  static_assert(sizeof(Data) == 16, "");

 public:
  Queue() : head_tail_(Tag{}, Tag{}), buffer_(kBufferSize) {
    fprintf(stderr, "buffer_wrap_delta_: %lu\n", Tag::kBufferWrapDelta);
    Tag tag;
    tag.mark_as_consumer();
    for (size_t i = 0; i < buffer_.size(); i++) {
      fprintf(stderr, "i: %zu, tag: %lx\n", i, tag.value);
      buffer_[tag.to_index()].tag = tag;
      ++tag;
    }
    std::atomic_thread_fence(std::memory_order::release);
  }
  Queue(const QueueOpts&) : Queue() {}

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
    __int128 expected_line = get_head_tail(std::memory_order::acquire)
                                 .line.load(std::memory_order::relaxed);
    fprintf(stderr, "> push_back()\n");
    HeadTail want;
    Tag claimed_tail_tag;
    do {
      HeadTail expected{expected_line};
      claimed_tail_tag = expected.tail_tag;
      if (claimed_tail_tag.value >=
          expected.head_tag.value + Tag::kBufferWrapDelta) {
        fprintf(stderr, "< push_back() #=> false\n");
        return false;
      }
      want.head_tag = expected.head_tag;
      ++want.tail_tag;
    } while (!head_tail_.get().line.compare_exchange_weak(
        expected_line, want.line, std::memory_order::release,
        std::memory_order::relaxed));

    Data new_data{/*value_=*/val, /*tag_=*/claimed_tail_tag};
    int idx = claimed_tail_tag.to_index();
    fprintf(stderr,
            "push_back head: %lu, tail: %lu, claimed_tail_tag: %lu, idx: "
            "%d/%zu, new_data.tag: %lu\n",
            want.head_tag, want.tail_tag, claimed_tail_tag, idx,
            buffer_.size(), new_data.tag);
    //__int128 expected_data_line = 0;
    __int128 expected_data_line =
        buffer_[idx].line.load(std::memory_order::acquire);
    fprintf(stderr, "actual tag: %lu\n", Data{expected_data_line}.tag);
    do {
      Data expected_data{/*line=*/expected_data_line};
      expected_data.tag = claimed_tail_tag;
      expected_data_line = expected_data.line.load(std::memory_order::relaxed);
    } while (!buffer_[idx].line.compare_exchange_weak(
        expected_data_line, new_data.line, std::memory_order::release,
        std::memory_order::relaxed));

    fprintf(stderr, "< push_back() #=> true\n");
    return true;
  }

  T pop_front() {
    fprintf(stderr, "> pop_front()\n");
    __int128 expected_line = get_head_tail(std::memory_order::acquire)
                                 .line.load(std::memory_order::relaxed);
    HeadTail want;
    Tag claimed_head_tag;
    do {
      HeadTail expected{expected_line};
      if (expected.head_tag == expected.tail_tag) {
        fprintf(stderr, "< pop_front() #=> empty\n");
        return T{};
      }
      claimed_head_tag = expected.head_tag;
      ++want.head_tag;
      want.tail_tag = expected.tail_tag;
    } while (!head_tail_.get().line.compare_exchange_weak(
        expected_line, want.line, std::memory_order::release,
        std::memory_order::relaxed));

    claimed_head_tag.mark_as_consumer();

    int idx = claimed_head_tag.to_index();
    fprintf(stderr,
            "pop_front: claimed_head_tag: %lu, want.head_tag: %lu, "
            "want.tail_tag: %lu, idx: %d\n",
            claimed_head_tag.value, want.head_tag.value, want.tail_tag.value,
            idx);
    while (true) {
      __int128 expected_data_line =
          buffer_[idx].line.load(std::memory_order::acquire);
      Data expected_data{/*line=*/expected_data_line};
      if (expected_data.tag == claimed_head_tag) {
        buffer_[idx].tag_atomic.store(claimed_head_tag,
                                      std::memory_order::release);
        return expected_data.value;
      }
      CHECK(claimed_head_tag >= expected_data.tag)
          << "claimed: " << claimed_head_tag.value << ", expected_data.tag: "
          << expected_data.tag.value;
      //fprintf(stderr, "expected: %lu, claimed: %lu\n", expected_data.tag,
      //        claimed_head_tag);
    }
  }

  size_t size() const {
    auto ht = get_head_tail();
    return (ht.tail_tag.value - ht.head_tag.value) / Tag::kIncrement;
  }

  static constexpr size_t capacity() { return kBufferSize; }

 private:
  ExclusiveCacheLine<HeadTail> head_tail_;
  std::vector<Data> buffer_;

  HeadTail get_head_tail(
      std::memory_order mem_order = std::memory_order::acquire) const {
    return HeadTail{head_tail_.get().line.load(mem_order)};
  }
};

template <typename T>
static constexpr bool memset0_to_bool() {
  T t;
  memset(&t, 0, sizeof(T));
  return static_cast<bool>(t);
}

template <typename T>
concept ZeroableAtomType = requires(T t) {
  std::is_trivially_constructible<T>::value;
  can_be_atomic<T>;
  static_cast<bool>(T{}) == false;
  memset0_to_bool<T>() == false;
};

// Multiple-producer, single-consumer.
// If more than one consumer exists at once, no items will be lost, but it is
// possible for events to appear out of order. This requires that no producer
// adds a "zero" item.
template <ZeroableAtomType T>
class MPSCQueue {
 public:
  static constexpr size_t next_pow_2(int v) {
    if ((v & (v - 1)) == 0) {
      return v;
    }
    int lg_v = 8 * sizeof(v) - __builtin_clz(v);
    return 1 << lg_v;
  }

  MPSCQueue(QueueOpts opts)
      : ht_(/*head=*/0, /*tail=*/0), buf_(next_pow_2(opts.max_size())) {
    CHECK(capacity());
  }

  ~MPSCQueue() {
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
    uint32_t head, tail;
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

      if (tail == buf_.size() - 1) {
        tail = 0;
      } else {
        tail++;
      }
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail{head, tail}.line.load(std::memory_order::relaxed),
        std::memory_order::release, std::memory_order::relaxed));

    uint32_t index = HeadTail{expected}.tail;

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
      uint32_t head;
      uint32_t tail;
    };
    std::atomic<uint64_t> line;

    HeadTail(uint64_t line) : line(line) {}
    HeadTail(uint32_t head, uint32_t tail) : head(head), tail(tail) {}
  } ht_;

  std::vector<std::atomic<T>> buf_;

  static inline constexpr size_t size(uint64_t line, size_t buf_size) {
    uint32_t head = HeadTail(line).head;
    uint32_t tail = HeadTail(line).tail;
    if (tail < head) {
      tail += buf_size;
    }
    return tail - head;
  }

  std::optional<uint32_t> reserve_for_pop() {
    uint64_t expected;
    uint32_t head, tail;
    do {
      expected = ht_.line.load(std::memory_order::acquire);
      if (size(expected, buf_.size()) == 0) {
        return {};
      }

      head = HeadTail(expected).head + 1;
      tail = HeadTail(expected).tail;

      if (head >= buf_.size()) {
        head -= buf_.size();
      }
    } while (!ht_.line.compare_exchange_weak(
        expected, HeadTail(head, tail).line.load(std::memory_order::relaxed),
        std::memory_order::release, std::memory_order::relaxed));

    return HeadTail(expected).head;
  }
};

}  // namespace theta
