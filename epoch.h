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

  std::mutex mutex_;
  std::list<Page*> old_pages_;
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
    return new (ptr) T{std::forward<Args>(args)...};
  }

  template <typename T, typename... Args>
  T* allocate(Args... args) {
    T* ptr = reinterpret_cast<T*>(
        pools_[get_local_cpu()].allocate(sizeof(T), alignof(T)));
    return new (ptr) T{std::forward<Args>(args)...};
  }

 private:
  std::vector<MemoryPool> pools_;
  // This will keep the pools from the next epoch alive until this one is
  // destroyed. Using a mutex to allow cross-thread communication.
  AllocatorPointer next_epoch_{nullptr};
};

class Epoch {
 public:
  static AllocatorPointer get_allocator();

  static void new_epoch();

 private:
  static Epoch& get_instance();

  hsp::KeepAlive<CPULocalMemoryPools> pools_{nullptr};
  std::mutex mutex_;

  Epoch() : pools_(new CPULocalMemoryPools{}) {}

  void new_epoch_impl();
};

template <typename T>
class EpochPtr {
 public:
  EpochPtr() : t_(nullptr), allocator_(nullptr) {}

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
