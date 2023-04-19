#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>
#include <utility>

#include "HyperSharedPointer.h"

namespace theta {

size_t get_local_cpu();

class CPULocalMemoryPools;
using AllocatorPointer = hsp::HyperSharedPointer<CPULocalMemoryPools>;

static constexpr size_t kPageSize = 4096;

struct alignas(kPageSize) Page {
  std::array<std::byte, kPageSize> data;
};

class alignas(sizeof(__int128)) MemoryPool {
 public:
  MemoryPool() {}

  ~MemoryPool() {
    std::lock_guard lock{mutex_};

    while (!dtors_.empty()) {
      dtors_.back()();
      dtors_.pop_back();
    }

    for (Page* page : old_pages_) {
      delete page;
    }

    delete d_.page.load(std::memory_order_relaxed);
  }

  template <typename T, typename... Args>
  T* allocate(Args... args) {
    T* ptr = reinterpret_cast<T*>(allocate(sizeof(T), alignof(T)));
    return new (ptr) T{std::forward<Args>(args)...};
  }

  void* allocate(size_t size, size_t alignment) {
    while (true) {
      Data data{d_};
      auto expected = d_.line.load(std::memory_order_relaxed);
      size_t want_offset = data.offset + size;
      if (alignment > 0) {
        size_t remainder = data.offset % alignment;
        if (remainder) {
          want_offset += alignment - remainder;
        }
      }

      if (want_offset > kPageSize) {
        std::lock_guard lock{mutex_};
        auto* old_page = data.page.load(std::memory_order_relaxed);
        data = d_;
        if (old_page == data.page.load(std::memory_order_relaxed)) {
          old_pages_.push_back(old_page);
          num_old_pages_.fetch_add(1, std::memory_order_relaxed);
          data.page.store(new Page, std::memory_order_relaxed);
          data.offset.store(0, std::memory_order_relaxed);
          d_.line.store(data.line.load(std::memory_order_relaxed),
                        std::memory_order_release);
        }
      }

      data.offset.store(want_offset, std::memory_order_relaxed);
      if (d_.line.compare_exchange_weak(
              expected, data.line.load(std::memory_order_relaxed),
              std::memory_order_release, std::memory_order_relaxed)) {
        void* page = data.page.load(std::memory_order_relaxed);
        return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(page) +
                                       want_offset - size);
      }
    }
  }

  void defer_dtor(std::function<void()> dtor) {
    // TODO(lpe): asan wants all calls to placement new to be paired with a
    // call to placement delete, which is what this function would do. However,
    // since the memory is in a bump allocator, that's not always necessary.
    // The Task object, which has a mutex member, is not trivially
    // destructible, so asan wants to see a placement delete. Unfortunately,
    // doing this is over a 10% performance degredation.
#if defined(DEBUG)
    std::lock_guard lock{mutex_};
    dtors_.push_back(dtor);
#endif
  }

  int num_pages() const {
    return 1 + num_old_pages_.load(std::memory_order_acquire);
  }

 private:
  union Data {
    struct {
      std::atomic<Page*> page;
      std::atomic<size_t> offset;
    };
    std::atomic<__int128> line;

    Data(const Data& other) {
      line.store(other.line.load(std::memory_order_acquire),
                 std::memory_order_relaxed);
    }

    Data& operator=(const Data& other) {
      line.store(other.line.load(std::memory_order_acquire));
      return *this;
    }

    Data() {
      page.store(new Page, std::memory_order_relaxed);
      offset.store(0, std::memory_order_relaxed);
    }
  } d_;

  std::atomic<int> num_old_pages_{0};
  std::mutex mutex_;
  std::list<Page*> old_pages_;
  std::list<std::function<void()>> dtors_;
};

class CPULocalMemoryPools {
  friend class Epoch;

 public:
  CPULocalMemoryPools() : pools_(std::thread::hardware_concurrency()) {}

  ~CPULocalMemoryPools() {
    std::atomic_thread_fence(std::memory_order::acquire);
    next_epoch_.reset();
  }

  template <typename T, typename... Args>
  T* allocate_on_cpu(size_t local_cpu, Args... args) {
    T* ptr =
        reinterpret_cast<T*>(pools_[local_cpu].allocate(sizeof(T), alignof(T)));
    auto* p = new (ptr) T{std::forward<Args>(args)...};

    if constexpr (!std::is_trivially_destructible<T>{}) {
      pools_[local_cpu].defer_dtor([p]() { p->~T(); });
    }

    return ptr;
  }

  template <typename T, typename... Args>
  T* allocate(Args... args) {
    return allocate_on_cpu<T>(get_local_cpu(), std::forward<Args>(args)...);
  }

 private:
  std::vector<MemoryPool> pools_;
  // This will keep the pools from the next epoch alive until this one is
  // destroyed.
  AllocatorPointer next_epoch_{nullptr};
};

class Epoch {
 public:
  static AllocatorPointer get_allocator();

  static void new_epoch();

 private:
  static Epoch& get_instance();

  std::atomic<int> epoch_number_{0};
  hsp::KeepAlive<CPULocalMemoryPools> pools_{nullptr};
  std::mutex mutex_;

  Epoch() : pools_(new CPULocalMemoryPools{}) {}

  void new_epoch_impl();
  void new_epoch_impl(const std::lock_guard<std::mutex>&);
};

template <typename T>
class EpochPtr {
 public:
  EpochPtr() : t_(nullptr), allocator_(nullptr) {}
  EpochPtr(std::nullptr_t) : t_(nullptr), allocator_(nullptr) {}

  using value_type = T;

  template <typename... Args>
  static EpochPtr<T> make(Args... args) {
    auto allocator = Epoch::get_allocator();
    T* t = allocator->allocate<T>(std::forward<Args>(args)...);
    return EpochPtr(t, std::move(allocator));
  }

  static EpochPtr<T>* to_address(EpochPtr<T>&& epoch_ptr) {
    return epoch_ptr.allocator()->template allocate<EpochPtr<T>>(
        std::move(epoch_ptr));
  }

  EpochPtr(EpochPtr&& other)
      : t_(other.t_), allocator_(std::move(other.allocator_)) {
    other.t_ = nullptr;
  }

  EpochPtr& operator=(const EpochPtr& other) {
    t_ = other.t_;
    allocator_ = other.allocator_;
    return *this;
  }

  EpochPtr(const EpochPtr& other)
      : t_(other.t_), allocator_(other.allocator_) {}

  EpochPtr& operator=(EpochPtr&& other) {
    t_ = other.t_;
    other.t_ = nullptr;
    allocator_ = std::move(other.allocator_);
    return *this;
  }

  ~EpochPtr() {
    t_ = nullptr;
    allocator_.reset();
  }

  void reset() {
    t_ = nullptr;
    allocator_.reset();
  }

  T& operator*() const { return *t_; }

  T* operator->() const { return t_; }

  explicit operator bool() const { return t_ != nullptr; }

  AllocatorPointer& allocator() { return allocator_; }

 private:
  T* t_;
  AllocatorPointer allocator_;

  EpochPtr(T* t, AllocatorPointer allocator)
      : t_(t), allocator_(std::move(allocator)) {}
};

}  // namespace theta
