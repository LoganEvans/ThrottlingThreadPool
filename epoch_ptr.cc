#include "epoch_ptr.h"

#include <rseq/rseq.h>

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
  return Epoch::get_instance().pools_.load(std::memory_order_acquire)->get();
}

/*static*/
void Epoch::new_epoch() {
  auto* new_ka =
      new hsp::KeepAlive<CPULocalMemoryPools>{new CPULocalMemoryPools{}};
  auto* old_ka =
      Epoch::get_instance().pools_.exchange(new_ka, std::memory_order_acq_rel);
  auto old_hsp = old_ka->get();
  {
    std::lock_guard lock{old_hsp->mutex_};
    old_hsp->next_epoch_ = new_ka->get();
  }
  delete old_ka;
}

/*static*/
Epoch& Epoch::get_instance() {
  static Epoch instance;
  return instance;
}

}  // namespace theta
