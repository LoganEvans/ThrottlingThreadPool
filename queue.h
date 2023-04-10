#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

template <typename T, size_t kNumQueues>
class Link {
 public:
  template <typename... Args>
  Link(T* t) : t_(t), prev_({}), next_({}) {}

  T* get() const { return t_; }

  bool remove() {
    // Mark this Link as invalid using a 16-byte compare-and-swap to dirty both
    // prev and next pointers.
    Pointers expected, desired;
    expected = {.joined =
                    p_[queue_index].joined.load(std::memory_order_relaxed)};
    while (true) {
      if (!expected.is_valid_non_atomic()) {
        return false;
      }

      desired.joined_non_atomic = p.joined_non_atomic;

      desired.prev_non_atomic = reinterpret_cast<Link*>(
          reinterpret_cast<uintptr_t>(desired_prev_non_atomic) | 1);
      desired.next_non_atomic = reinterpret_cast<Link*>(
          reinterpret_cast<uintptr_t>(desired_next_non_atomic) | 1);

      if (p_[queue_index].compare_exchange_weak(
              p_[queue_index].joined, desired.joined, std::memory_order_release,
              std::memory_order_relaxed)) {
        break;
      }
    }

    // The deletion must happen from front to back, and we may only modify the
    // Link in front of this one if it is not dirty. Since the head Link will
    // always be non-dirty, eventually this Link will have a prev neighbor that
    // is not dirty.
    Link* prev = expected.prev_non_atomic;
    Link* expected_next = this;
    while (true) {
      while (!Link::is_clean(expected_next)) {
        prev = prev.load(std::memory_order_acquire);
        expected_next = prev->p_.next.load(std::memory_order_acquire);
      }

      if (prev->p_.next.compare_exchange_weak(expected_next, next,
                                              std::memory_order_release,
                                              std::memory_order_aquire)) {
        break;
      }
    }

    // The next neighbor may now be modified. It is possible that the next
    // neighbor will change during this method call if we used push_back to add
    // a Link. If our next neighbor is dirty, it cannot progress past
    // attempting to modify its prev neighbor, so we can update it, although we
    // need to take care to make sure the pointer we write is dirty. If our
    // next neighbor is clean, updating the pointer is sufficient.
    //
    // Another situation that can happen is that after we finished modifying
    // our prev neighbor, that prev_neighbor was removed and has already
    // updated our next neighbor. This scenario is detectible by our next
    // neighbor not pointing at this.
    Link* next = expected.next_non_atomic;
    Link* desired_prev = expected.prev_non_atomic;
    Link* expected_prev = this;
    while (true) {
      if (next->p_.prev.compare_exchange_weak(expected_prev, desired_prev,
                                              std::memory_order_release,
                                              std::memory_order_acquire)) {
        break;
      }

      // Something went wrong.
      // 1) This might be a spurious failure because we used
      //    compare_exchange_weak.
      // 2) The next neighbor is attempting to delete and its pointers are now
      //    dirty.
      // 3) The prev neighbor finished deleting after we updated its next
      //    pointer, so now the next neighbor points at something else.
      // 4) We used push_back to insert a new Link after this one.

      if (Link::clean(expected_prev) == this && Link::is_dirty(expected_prev)) {
        // Scenario (2). The next neighbor cannot finish deleting until we
        // finish; we should be able to succeed if we write a dirty pointer.
        desired_prev = Link::dirty(desired_prev);
      } else if (Link::clean(expected_prev) != this) {
        Link* refreshed_next =
            Link::clean(p_.next.load(std::memory_order_aquire));
        if (refreshed_next != next) {
          // Scenario (3). There's nothing we need to do.
          break;
        } else {
          // Scenario (4). Try again with the new next neighbor. This scenario
          // can only happen once per removal.
          next = refreshed_next;
        }
      } else {
        // Scenario (1). Just try again.
      }
    }
  }

  // AKA push_front. This should only be called on the tail Link.
  void push_head(Link* link) {
    
  }

  // AKA push_back. This should only be called on the tail Link.
  void push_tail(Link* link) {
    
  }

 private:
  union alignas(sizeof(__int128)) Pointers {
    struct {
      std::atomic<Link*> prev{nullptr};
      std::atomic<Link*> next{nullptr};
    };
    struct {
      Link* prev_non_atomic;
      Link* next_non_atomic;
    };
    std::atomic<__int128_t> joined;
    __int128_t joined_non_atomic;

    bool is_clean_non_atomic() const { return is_clean(next_non_atomic); }

    static Link* clean(Link* link) {
      return reinterpret_cast<Link*>(reinterpret_cast<uintptr_t>(link) & ~1ULL);
    }

    static bool is_clean(Link* link) { return link == clean(link); }

    static Link* dirty(Link* link) {
      return reinterpret_cast<Link*>(reinterpret_cast<uintptr_t>(link) | 1ULL);
    }

    static bool is_dirty(Link* link) { return link == dirty(link); }
  } p_;
  static_assert(sizeof(p_) == sizeof(__int128_t), "");

  T* t_;
  void* __padding_;
};
static_assert(sizeof(Link<int>) == 32, "");

template <typename T, size_t kNumQueues, size_t kQueueIndex>
class Queue {
 public:
  using Link = Link<T, kNumQueues>;

  void push_back(Link* val);
  void push_front(Link* val);

  Link* maybe_pop_back();
  Link* maybe_pop_front();

  Link* wait_pop_back();
  Link* wait_pop_front();

  void remove(Link* link);

 private:
  // TODO(lpe): Get rid of this mutex. It's here to separate out phases of the
  // development process.
  std::mutex dev_mutex_;

  Link* head_;
  Link* tail_;
};
