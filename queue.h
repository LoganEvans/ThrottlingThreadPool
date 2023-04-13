#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "epoch.h"

template <typename T>
class Link {
 public:
  Link(T* t, AllocatorPointer allocator)
      : t_(t), allocator_(std::move(allocator)) {}

  Link(T* t = nullptr) : Link(t, Epoch::get_allocator()) {}

  void destroy() ~Link() { allocator_.reset(); }

  T* get() const { return t_; }

  T* remove() { return nullptr; }

  // Appends a Link to the list.
  void push(T* t) {}

 private:
  std::atomic<Link*> prev{nullptr};
  std::atomic<Link*> next{nullptr};
  std::atomic<T*> t_;
  AllocatorPointer allocator_;
};

template <typename T>
class Queue {
 public:
  using Link = Link<T>;

  Queue() { head_.push_head(tail_); }

  void push_back(T* val) {
    Link* link{val};

    while (true) {
      Link* tail = tail_.load(std::memory_order_acquire);
      Link* tail_next;
      // tail_ can be stale, so march to the end of the list.
      while (tail_next = tail->next.load(std::memory_order_acquire)) {
        tail = tail_next;
      }
      link->prev.store(tail, std::memory_order_release);

      Link* expected{nullptr};
      if (tail->next.compare_exchange_weak(expected, link,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
        // This operation races with other calls to push_back, which is why
        // it's important to check if tail_ is stale.
        tail_.store(link, std::memory_order_release);
        break;
      }
    }
  }

  void push_front(T* val) {
    Link* link{val};

    while (true) {
      Link* head = head_.load(std::memory_order_acquire);
      Link* head_prev;
      // head_ can be stale, so march to the beginning of the list.
      while (head_prev = head->prev.load(std::memory_order_acquire)) {
        head = head_prev;
      }
      link->next.store(head, std::memory_order_release);

      Link* expected{nullptr};
      if (head->next.compare_exchange_weak(expected, link,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
        // This operation races with other calls to push_front, which is why
        // it's important to check if head_ is stale.
        head_.store(link, std::memory_order_release);
        break;
      }
    }
  }

  T* pop_front();

  T* pop_back();

 private:
  union {
   struct {
     std::atomic<Link*> head_;
     std::atomic<Link*> tail_;
   };
   std::atomic<__int128> line_{0};
  };
};

