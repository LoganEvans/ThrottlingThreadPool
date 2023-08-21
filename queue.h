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
constexpr std::size_t hardware_destructive_interference_size = 128;
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

template <AtomType T, size_t kBufferSize = 128>
class Queue {
  struct Tag {
    static_assert((kBufferSize & (kBufferSize - 1)) == 0, "");

    // Assuming sizeof(Data) == 16, kIncrement will make it so that adjacent
    // values will always be on separate cache lines.
    // XXX
    //static constexpr uint64_t kIncrement =
    //    1 + hardware_destructive_interference_size / 16;
    static constexpr uint64_t kIncrement = 1;
    static constexpr uint64_t kBufferWrapDelta = kBufferSize * kIncrement;
    static constexpr uint64_t kBufferSizeMask = kBufferSize - 1;
    static constexpr uint64_t kConsumerFlag = (1ULL << 63);
    static constexpr uint64_t kWaitingFlag = (1ULL << 62);

    uint64_t raw;

    Tag(uint64_t raw) : raw(raw) {}
    Tag() : Tag(0) {}

    Tag& operator++() {
      raw += kIncrement;
      return *this;
    }

    Tag& operator++(int) {
      raw += kIncrement;
      return *this;
    }

    Tag& operator--() {
      raw -= kIncrement;
      return *this;
    }

    Tag& operator--(int) {
      raw -= kIncrement;
      return *this;
    }

    auto operator<=>(const Tag&) const = default;

    std::string DebugString() const {
      return "Tag<" + std::string(is_producer() ? "P" : "C") +
             (is_waiting() ? std::string("|W") : "") + ">{" +
             std::to_string(value()) + "@" + std::to_string(to_index()) + "}";
    }

    uint64_t value() const { return (raw << 2) >> 2; }

    Tag prev_paired_tag() const {
      if (is_consumer()) {
        return Tag{(raw ^ kConsumerFlag) & ~kWaitingFlag};
      } else {
        return Tag{((raw - kBufferWrapDelta) ^ kConsumerFlag) & ~kWaitingFlag};
      }
    }

    bool is_paired(Tag observed_tag) const {
      return prev_paired_tag().raw == (observed_tag.raw & ~kWaitingFlag);
    }

    bool is_producer() const { return (raw & kConsumerFlag) == 0; }

    void mark_as_producer() { raw &= ~kConsumerFlag; }

    bool is_consumer() const { return (raw & kConsumerFlag) > 0; }

    void mark_as_consumer() { raw |= kConsumerFlag; }

    void mark_as_waiting() { raw |= kWaitingFlag; }

    bool is_waiting() const { return (raw & kWaitingFlag) > 0; }

    void clear_waiting_flag() { raw &= ~kWaitingFlag; }

    int to_index() const { return raw & kBufferSizeMask; }
  };
  static_assert(sizeof(Tag) == sizeof(uint64_t), "");

  union Index {
    struct {
      Tag tag;
    };
    struct {
      std::atomic<Tag> tag_atomic;
    };
    struct {
      std::atomic<uint64_t> tag_raw_atomic;
    };

    Index(Tag tag) : tag(tag) {}
    Index() : Index(/*tag=*/0) {}

    std::string DebugString() const {
      return "Index{tag=" + tag.DebugString() + "}";
    }
  };
  static_assert(sizeof(Index) == sizeof(Tag), "");

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

    Data(T value, Tag tag) : value(value), tag(tag) {}
    Data(__int128 line) : line(line) {}
    Data() : Data(/*line=*/0) {}
    Data(const Data& other)
        : Data(other.line.load(std::memory_order::relaxed)) {}
    Data& operator=(const Data& other) {
      line.store(other.line.load(std::memory_order::relaxed),
                 std::memory_order::relaxed);
      return *this;
    }

    std::string DebugString() const {
      return "Data{value=" + std::string(bool(value) ? "####" : "null") + ", " +
             tag.DebugString() + "}";
    }
  };
  static_assert(sizeof(Data) == sizeof(Data::line), "");
  static_assert(sizeof(Data) == 16, "");

 public:
  Queue()
      : head_(Tag::kBufferWrapDelta),
        tail_(Tag::kBufferWrapDelta),
        buffer_(kBufferSize) {
    Tag tag;
    tag.mark_as_consumer();
    for (size_t i = 0; i < buffer_.size(); i++) {
      buffer_[tag.to_index()].tag = tag;
      ++tag;
    }
    std::atomic_thread_fence(std::memory_order::release);
  }
  Queue(const QueueOpts&) : Queue() {}

  ~Queue() {
    while (true) {
      auto v = try_pop();
      if (!v) {
        break;
      }
    }
  }

  void push(T val) {
    Tag tail{tail_.tag_raw_atomic.fetch_add(Tag::kIncrement,
                                            std::memory_order::acq_rel)};
    do_push(std::move(val), tail);
  }

  bool try_push(T val) {
    const Tag head{head_.tag_atomic.load(std::memory_order::acquire)};

    Tag expected_tail{head};
    Tag desired_tail{expected_tail};
    desired_tail++;

    while (!tail_.tag_atomic.compare_exchange_weak(
        expected_tail, desired_tail, std::memory_order::release,
        std::memory_order::relaxed)) {
      desired_tail = expected_tail;
      desired_tail++;
      if (desired_tail.raw >= head.raw + Tag::kBufferWrapDelta) {
        return false;
      }
    }

    do_push(std::move(val), expected_tail);
    return true;
  }

  T pop() {
    Tag tag{/*raw=*/head_.tag_raw_atomic.fetch_add(Tag::kIncrement,
                                                   std::memory_order::acq_rel)};
    tag.mark_as_consumer();
    return do_pop(tag);
  }

  std::optional<T> try_pop() {
    const Tag tail{tail_.tag_atomic.load(std::memory_order::acquire)};

    Tag desired_head{tail.raw};
    Tag expected_head{desired_head.raw - Tag::kIncrement};

    while (!head_.tag_atomic.compare_exchange_weak(
        expected_head, desired_head, std::memory_order::release,
        std::memory_order::relaxed)) {
      desired_head = expected_head;
      desired_head++;
      if (desired_head > tail) {
        return {};
      }
    }

    expected_head.mark_as_consumer();
    return do_pop(expected_head);
  }

  size_t size() const {
    // Reading head before tail will make it possible to "see" more elements in
    // the queue than it can hold, but this makes it so that the size will
    // never be negative.
    auto head = head_.tag_atomic.load(std::memory_order::acquire);
    auto tail = tail_.tag_atomic.load(std::memory_order::acquire);

    return (tail.raw - head.raw) / Tag::kIncrement;
  }

  static constexpr size_t capacity() { return kBufferSize; }

 private:
  alignas(hardware_destructive_interference_size) Index head_;
  alignas(hardware_destructive_interference_size) Index tail_;
  alignas(hardware_destructive_interference_size) std::vector<Data> buffer_;

  void do_push(T val, const Tag& tag) {
    DCHECK(tag.is_producer());
    DCHECK(!tag.is_waiting());

    int idx = tag.to_index();

    // This is the strangest issue -- with Ubuntu clang version 15.0.7,
    // when observed_data is defined inside of the loop scope, benchmarks will
    // slow down by over 5x.
    Data observed_data;
    while (true) {
      __int128 observed_data_line =
          buffer_[idx].line.load(std::memory_order::acquire);
      observed_data = Data{/*line=*/observed_data_line};

      if (tag.is_paired(observed_data.tag)) {
        break;
      }

      wait_for_data(tag, observed_data.tag);
    }

    Data new_data{/*value_=*/val, /*tag_=*/tag};
    Data old_data{buffer_[idx].line.exchange(
        new_data.line.load(std::memory_order::relaxed),
        std::memory_order::acq_rel)};
    if (old_data.tag.is_waiting()) {
      buffer_[idx].tag_atomic.notify_all();
    }
  }

  T do_pop(const Tag& tag) {
    DCHECK(tag.is_consumer());
    DCHECK(!tag.is_waiting());

    int idx = tag.to_index();

    Data observed_data;
    while (true) {
      observed_data =
          Data{/*line=*/buffer_[idx].line.load(std::memory_order::acquire)};

      if (tag.is_paired(observed_data.tag)) {
        break;
      }

      wait_for_data(tag, observed_data.tag);
    }

    // This is another strange issue -- it is faster to exchange the __int128
    // value backing the Data object instead of just exchanging the 8 byte tag
    // value inside of that Data object.
    Data old_data{buffer_[idx].line.exchange(
        Data{/*value=*/T{}, /*tag=*/tag}.line.load(std::memory_order::relaxed),
        std::memory_order::acq_rel)};

    if (old_data.tag.is_waiting()) {
      buffer_[idx].tag_atomic.notify_all();
    }

    return observed_data.value;
  }

  void wait_for_data(const Tag& claimed_tag, Tag observed_tag) {
    int idx = claimed_tag.to_index();
    while (true) {
      Tag want_tag{observed_tag};
      want_tag.mark_as_waiting();
      if ((observed_tag == want_tag) ||
          buffer_[idx].tag_atomic.compare_exchange_weak(
              observed_tag, want_tag, std::memory_order::release,
              std::memory_order::relaxed)) {
        buffer_[idx].tag_atomic.wait(want_tag, std::memory_order::acquire);
        break;
      }

      if (claimed_tag.is_paired(observed_tag)) {
        break;
      }
    }
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
      auto v = try_pop();
      if (!v) {
        break;
      }
    }
  }

  bool try_push(T val) { return try_push(val, nullptr); }

  bool try_push(T val, size_t* num_items) {
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

  std::optional<T> try_pop() {
    auto maybe_index = reserve_for_pop();
    if (!maybe_index.has_value()) {
      return {};
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

  union alignas(hardware_destructive_interference_size) HeadTail {
    struct {
      uint32_t head;
      uint32_t tail;
    };
    std::atomic<uint64_t> line;

    HeadTail(uint64_t line) : line(line) {}
    HeadTail(uint32_t head, uint32_t tail) : head(head), tail(tail) {}
  } ht_;

  alignas(
      hardware_destructive_interference_size) std::vector<std::atomic<T>> buf_;

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
