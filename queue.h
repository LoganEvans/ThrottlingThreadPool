#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "epoch.h"

namespace theta {

template <typename EpochPtrT>
class Queue {
 public:
  class Link {
    template <typename U>
    friend class Queue;

   public:
    Link(EpochPtrT t, AllocatorPointer allocator)
        : t_(std::move(t)), allocator_(std::move(allocator)) {}

    EpochPtrT destroy() {
      EpochPtrT tmp = std::move(t_);
      allocator_.reset();
      return tmp;
    }

   //private:
    std::atomic<Link*> next_{nullptr};
    EpochPtrT t_;
    AllocatorPointer allocator_;
  };

  Queue() {}

  void push_back(EpochPtrT val) {
    auto allocator = Epoch::get_allocator();
    Link* link =
        allocator->allocate<Link>(std::move(val), std::move(allocator));

  START:
    while (true) {
      Link* cursor = head_.load(std::memory_order::acquire);
      if (cursor == nullptr) {
        if (head_.compare_exchange_weak(cursor, link,
                                        std::memory_order::release,
                                        std::memory_order::relaxed)) {
          return;
        }
        continue;
      }

      Link* cursor_next = cursor->next_.load(std::memory_order_acquire);

      while (true) {
        cursor_next = cursor->next_.load(std::memory_order::acquire);
        if (cursor == cursor_next) {
          // Another thread is in the middle of pop_back.
          goto START;
        }
        if (!cursor_next) {
          break;
        }
        cursor = cursor_next;
      }

      if (cursor->next_.compare_exchange_strong(cursor_next, link,
                                                std::memory_order::release,
                                                std::memory_order::relaxed)) {
        return;
      }
    }
  }

  void push_front(EpochPtrT val) {
    auto allocator = Epoch::get_allocator();
    Link* link =
        allocator->allocate<Link>(std::move(val), std::move(allocator));

    while (true) {
      Link* head = head_.load(std::memory_order::relaxed);
      link->next_.store(head, std::memory_order::release);
      if (head_.compare_exchange_weak(head, link, std::memory_order::release,
                                      std::memory_order::relaxed)) {
        break;
      }
    }
  }

  EpochPtrT pop_front() {
    std::shared_lock lock{kludge_};

    while (true) {
      Link* cursor = head_.load(std::memory_order::acquire);
      if (!cursor) {
        return EpochPtrT{};
      }

      Link* cursor_next = cursor->next_.load(std::memory_order::acquire);
      if (cursor == cursor_next) {
        // We had a list with 2 elements, and while pop_back is attempting to
        // destroy the last element, pop_front destroyed the first one and is
        // now looking at the one pop_back is working on.
        continue;
      }

      if (head_.compare_exchange_weak(cursor, cursor_next,
                                      std::memory_order::release,
                                      std::memory_order::relaxed)) {
        return cursor->destroy();
      }
    }
  }

  EpochPtrT pop_back() {
    std::unique_lock lock{kludge_};

    while (true) {
      Link* cursor = head_.load(std::memory_order::acquire);
      if (cursor == nullptr) {
        return EpochPtrT{};
      }

      Link* cursor_next = cursor->next_.load(std::memory_order::acquire);
      if (!cursor_next) {
        if (head_.compare_exchange_strong(cursor, nullptr,
                                          std::memory_order::release,
                                          std::memory_order::relaxed)) {
          return cursor->destroy();
        } else {
          continue;
        }
      }

      Link* cursor_next_next;
      do {
        cursor_next_next =
            cursor_next->next_.load(std::memory_order::acquire);
        if (cursor_next_next) {
          cursor = cursor_next;
          cursor_next = cursor_next_next;
        }
      } while (cursor_next_next);

      if (!cursor_next->next_.compare_exchange_strong(
              cursor_next_next, cursor_next, std::memory_order::release,
              std::memory_order::acquire)) {
        continue;
      }

      cursor->next_.store(nullptr, std::memory_order::release);

      return cursor_next->destroy();
    }
  }

 private:
  std::atomic<Link*> head_{nullptr};
  // This prevents pop_front and pop_back from opperating at the same time.
  std::shared_mutex kludge_;
};

}  // namespace theta
