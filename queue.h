#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

template <typename T>
class Link {
  static Link* clean(Link* link) {
    return reinterpret_cast<Link*>(reinterpret_cast<uintptr_t>(link) & ~1ULL);
  }

  static bool is_clean(Link* link) { return link == clean(link); }

  static Link* dirty(Link* link) {
    return reinterpret_cast<Link*>(reinterpret_cast<uintptr_t>(link) | 1ULL);
  }

  static bool is_dirty(Link* link) { return link == dirty(link); }

 public:
  template <typename... Args>
  Link(T* t) : t_(t) {}

  T* get() const { return t_; }

  bool remove() {
    // Mark this Link as invalid using a 16-byte compare-and-swap to dirty both
    // prev and next pointers.
    Pointers expected, desired;
    expected = {.joined = p_.joined.load(std::memory_order_relaxed)};
    while (true) {
      if (!expected.is_valid_non_atomic()) {
        return false;
      }

      desired.joined_non_atomic = p_.joined_non_atomic;

      desired.prev_non_atomic = reinterpret_cast<Link*>(
          reinterpret_cast<uintptr_t>(desired.prev_non_atomic) | 1);
      desired.next_non_atomic = reinterpret_cast<Link*>(
          reinterpret_cast<uintptr_t>(desired.next_non_atomic) | 1);

      if (p_.compare_exchange_weak(p_.joined, desired.joined,
                                   std::memory_order_release,
                                   std::memory_order_relaxed)) {
        break;
      }
    }

    // The deletion must happen from front to back, and we may only modify the
    // Link in front of this one if it is not dirty. Since the head Link will
    // always be non-dirty, eventually this Link will have a prev neighbor that
    // is not dirty.
    Link* prev = expected.prev_non_atomic;
    Link* next = expected.next_non_atomic;
    Link* expected_next = this;
    while (true) {
      if (prev->p_.next.compare_exchange_weak(expected_next, next,
                                              std::memory_order_release,
                                              std::memory_order_acquire)) {
        break;
      }

      while (!Link::is_clean(expected_next)) {
        prev = expected.prev.load(std::memory_order_acquire);
        expected_next = prev->p_.next.load(std::memory_order_acquire);
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
    // updated our next neighbor. This scenario is detectable by our next
    // neighbor not pointing at this.
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
            Link::clean(p_.next.load(std::memory_order_acquire));
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
    DCHECK(p_.prev_non_atomic == nullptr)
        << "push_head may only be called on the head node";
    DCHECK(!link->p_.joined_non_atomic) << "Link nodes may not be reused";

    // Update head's link.
    Link* expected = this->next.load(std::memory_order_relaxed);
    Link* desired = link;
    link->prev.store(this, std::memory_order_release);
    while (true) {
      link->next.store(expected, std::memory_order_release);
      if (p_.compare_exchange_weak(expected, desired, std::memory_order_release,
                                   std::memory_order_relaxed)) {
        break;
      }
    }

    // Update the previous first element's link.
    Link* elem = expected;
    expected = this;
    while (true) {
      if (elem->prev.compare_exchange_weak(expected, desired,
                                       std::memory_order_release,
                                       std::memory_order_relaxed)) {
        break;
      }
      CHECK(Link::clean(expected) == this);
      if (Link::is_dirty(expected)) {
        desired = Link::dirty(desired);
      }
    }
  }

  // AKA push_back. This should only be called on the tail Link.
  void push_tail(Link* link) {
    DCHECK(p_.next_non_atomic == nullptr)
        << "push_tail may only be called on the tail node";
    DCHECK(!link->p_.joined_non_atomic) << "Link nodes may not be reused";

    // Update tail's link.
    Link* expected = this->prev.load(std::memory_order_relaxed);
    Link* desired = link;
    link->next.store(this, std::memory_order_release);
    while (true) {
      link->prev.store(expected, std::memory_order_release);
      if (p_.compare_exchange_weak(expected, desired, std::memory_order_release,
                                   std::memory_order_relaxed)) {
        break;
      }
    }

    // Update the previous first element's link.
    Link* elem = expected;
    expected = this;
    while (true) {
      if (elem->next.compare_exchange_weak(expected, desired,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
        break;
      }
      CHECK(Link::clean(expected) == this);
      if (Link::is_dirty(expected)) {
        desired = Link::dirty(desired);
      }
    }
  }

 private:
  union alignas(sizeof(__int128)) Pointers {
    Pointers() : joined_non_atomic(static_cast<__int128>(0)) {}

    struct {
      std::atomic<Link*> prev{nullptr};
      std::atomic<Link*> next{nullptr};
    };
    struct {
      Link* prev_non_atomic;
      Link* next_non_atomic;
    };
    std::atomic<__int128> joined;
    __int128 joined_non_atomic;

    bool is_clean_non_atomic() const { return is_clean(next_non_atomic); }
  } p_;
  static_assert(sizeof(p_) == sizeof(__int128), "");

  T* const t_;
  void* __padding_;
};
static_assert(sizeof(Link<int>) == 32, "");

template <typename T>
class Queue {
 public:
  using Link = Link<T>;

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
