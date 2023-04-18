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
  return Epoch::get_instance().pools_.get();
}

/*static*/
void Epoch::new_epoch() { get_instance().new_epoch_impl(); }

void Epoch::new_epoch_impl() {
  std::lock_guard lock{mutex_};
  auto old_epoch = pools_.get();
  auto new_epoch = pools_.reset(new CPULocalMemoryPools{});
  old_epoch->next_epoch_ = std::move(new_epoch);
}

/*static*/
Epoch& Epoch::get_instance() {
  static Epoch instance;
  return instance;
}

}  // namespace theta
