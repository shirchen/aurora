package org.apache.aurora.scheduler.state;

import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.OfferManager;

import javax.inject.Inject;

/**
 * Abstraction for killing tasks.
 */

public interface KillManager {

  void killTask(String taskId);


  class KillManagerImpl implements KillManager {

    private final Driver driver;
    private final OfferManager offerManager;


    @Inject
    KillManagerImpl(
        Driver driver,
        OfferManager offerManager
    ) {
      this.driver = driver;
      this.offerManager = offerManager;
    }

    @Override
    public void killTask(String taskId) {
      offerManager.removeTaskId(taskId);
      driver.killTask(taskId);
    }
  }
}

