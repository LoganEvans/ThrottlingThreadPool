#include "epoch.h"

#include <glog/logging.h>
#include <rseq/rseq.h>

#include <mutex>
#include <set>

namespace theta {

size_t get_local_cpu() {
  thread_local int remainingUses = 0;
  thread_local size_t cpu = -1;

  if (remainingUses) {
    remainingUses--;
    return cpu;
  }

  remainingUses = 31;
  cpu = rseq_current_cpu();
  return cpu;
}

/*static*/
hsp::HyperSharedPointer<CPULocalMemoryPools> Epoch::get_allocator() {
  auto& instance = Epoch::get_instance();
  auto v = instance.pools_.get();
  int epoch_number = instance.epoch_number_.load(std::memory_order::relaxed);

  if (v->pools_[v.originalCpu()].num_pages() >= 4) {
    std::lock_guard lock{instance.mutex_};
    if (epoch_number == instance.epoch_number_.load(std::memory_order::acquire)) {
      instance.new_epoch_impl(lock);
    }
  }

  return instance.pools_.get();
}

/*static*/
void Epoch::new_epoch() { get_instance().new_epoch_impl(); }

void Epoch::new_epoch_impl() {
  std::lock_guard lock{mutex_};
  new_epoch_impl(lock);
}

void Epoch::new_epoch_impl(const std::lock_guard<std::mutex>&) {
  auto old_epoch = pools_.get();
  auto new_epoch = pools_.reset(new CPULocalMemoryPools{});
  old_epoch->next_epoch_ = std::move(new_epoch);
  epoch_number_.fetch_add(1, std::memory_order_acq_rel);
}

/*static*/
Epoch& Epoch::get_instance() {
  static Epoch instance;
  return instance;
}

}  // namespace theta
