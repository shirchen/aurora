package org.apache.aurora.scheduler.state;

import com.google.common.base.Optional;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.scheduling.TaskGroups;
import org.apache.aurora.scheduler.storage.ReservationStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Abstraction for killing tasks.
 */

public interface KillManager {

  void killTask(String taskId);


  class KillManagerImpl implements KillManager {

    private static final Logger LOG = LoggerFactory.getLogger(KillManager.class);

    private final Driver driver;
    private final Storage storage;


    @Inject
    KillManagerImpl(
        Driver driver,
        Storage storage
    ) {
      this.driver = driver;
      this.storage = storage;
    }

    @Override
    public void killTask(String taskId) {
//      reservationStore.removeTaskId(taskId);
      //TODO: Transform taskId to task key

      Optional<IScheduledTask> scheduledTask = storage.read(
          storeProvider -> storeProvider.getTaskStore().fetchTask(taskId));

      if (scheduledTask.isPresent()) {
        IScheduledTask iScheduledTask = scheduledTask.get();
        IJobKey iJobKey = Tasks.getJob(iScheduledTask);

        String fullKey = JobKeys.canonicalString(iJobKey) + "/" + Tasks.getInstanceId(iScheduledTask);

        storage.write(
            (Storage.MutateWork.NoResult.Quiet) storeProvider -> storeProvider.getReservationStore().removeTaskId(fullKey));
        driver.killTask(taskId);
      } else {
        LOG.error("Could not find taskId in TaskStore" + taskId);
      }
    }
  }
}

