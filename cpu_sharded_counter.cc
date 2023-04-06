#include "cpu_sharded_counter.h"

#include <glog/logging.h>
#include <rseq/rseq.h>
#include <sched.h>

namespace theta {

int get_cpu() {
  thread_local int remainingUses = 0;
  thread_local unsigned int cpu = -1;

  if (remainingUses) {
    remainingUses--;
    return cpu;
  }

  remainingUses = 31;
  cpu = rseq_current_cpu();
  return cpu;
}

}
