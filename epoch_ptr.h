#pragma once

#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>

#include "HyperSharedPointer.h"

namespace theta {

static constexpr size_t kMemoryPoolSize = 4096;

struct alignas(sysconf(_SC_PAGE_SIZE)) Page {
  std::array<std::byte, sysconf(_SC_PAGE_SIZE)> data;
};

class alignas(sizeof(__int128)) MemoryPool {
 public:
  ~MemoryPool() {
    std::lock_guard lock{mutex_};
    for (Page* page : old_pages_) {
      delete page;
    }
  }

  void* allocate(size_t size, size_t alignment = 0) {
    size_t padding = 0;
    while (true) {
      Data data{d_};
      auto expected = d_.all_data_.load(std::memory_order_relaxed);
      size_t want_offset = data.offset_ + size;
      if (alignment > 0) {
        size_t remainder = data.offset_ % alignment;
        if (remainder) {
          want_offset += alignment - remainder;
        }
      }

      if (want_offset > kMemoryPoolSize) {
        std::lock_guard lock{mutex_};
        auto* old_page = data.page_.load(std::memory_order_relaxed);
        data = d_;
        if (old_page == data.page_.load(std::memory_order_relaxed)) {
          old_pages_.push_back(old_page);
          data.page_.store(new Page, std::memory_order_relaxed);
          data.offset_.store(0, std::memory_order_relaxed);
          d_.all_data_.store(data.all_data_.load(std::memory_order_relaxed),
                         std::memory_order_release);
        }
      }

      data.offset_.store(want_offset, std::memory_order_relaxed);
      if (d_.all_data_.compare_exchange_weak(
              expected, data.all_data_.load(std::memory_order_relaxed),
              std::memory_order_release, std::memory_order_relaxed)) {
        break;
      }
    }
  }

 private:
  union Data {
    struct {
      std::atomic<Page*> page_;
      std::atomic<size_t> offset_;
    };
    std::atomic<__int128> line_{0};

    Data(const Data& other) {
      all_data_.store(other.all_data_.load(std::memory_order_acquire),
                  std::memory_order_relaxed);
    }

    Data& operator=(const Data& other) {
      all_data_.store(other.all_data_.load(std::memory_order_acquire));
      return *this;
    }
  } d_;

  std::mutex mutex_;
  std::list<Page*> old_pages_;
};
static_assert(sizeof(MemoryPool) == kMemoryPoolSize, "");

template <typename T>
class EpochPtr {
 public:
  T* get() const;
  void new_epoch();
  void defer(std::function<void()> func);

 private:
};

class EpochAllocatorBase {
 public:
 private:
  
};

}  // namespace theta
