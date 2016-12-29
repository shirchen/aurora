package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.*;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;


/**
 * A pubsub event subscriber that accepts an OfferAdded message and unreserves previously
 * dynamically reserved resources that are no longer needed.
 * Logic is as follows:
 *  We loop over the entire list of resources as part of that offer since not all resources may need
 *  to be unreserved.
 *  The we try to match the label which is a task name in form of 'role/env/job_name/0' to an
 * existing task that is in an active state but is not running.
 *  Why? We want to only unreserve resources part of an offer that came back and which tasks are not
 *  in process of being scheduled. We could get resources that come back after we have either killed
 *  a task or did some resource shaping: either decreased or increased necessary resources for a
 *  task.
 */
public class OfferReconciler implements PubsubEvent.EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(OfferReconciler.class);

  private final OfferManager offerManager;
  private final Storage storage;

  @Inject
  @VisibleForTesting
  public OfferReconciler(OfferManager offerManager, Storage storage)
  {
    this.offerManager = offerManager;
    this.storage = storage;
  }

  private static Query.Builder buildQuery(Protos.Label label) {
    // Now all tasks that are not in progress will be in PENDING, ASSIGNED, etc, but NOT
    // RUNNING. So if a task is in RUNNING, then unreserve. Otherwise, we may need this offer.
    // task_name is gonna be of 'role/env/job_name/0' format.
    String taskName = label.getValue();
    List<String> parsed = Splitter.on("/").splitToList(taskName);
    IJobKey jobKey2 = JobKeys.from(parsed.get(0), parsed.get(1), parsed.get(2));
    int instanceId = Integer.parseInt(parsed.get(3));

    return Query.instanceScoped(jobKey2, instanceId).activeNotRunning();
  }

  @Subscribe
  public void offerAdded(PubsubEvent.OfferAdded offerAdded) {
    HostOffer offer = offerAdded.getOffer();
    List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
    for (Protos.Resource resource: resourceList) {
      Protos.Resource.ReservationInfo resInfo = resource.getReservation();
      Protos.Labels labels = resInfo.getLabels();
      // Label list should only have one label, but this is defensive code.
      for (Protos.Label label: labels.getLabelsList()) {
        Iterable<IScheduledTask> foundTasks = storage.read(
            storeProvider -> storeProvider.getTaskStore().fetchTasks(buildQuery(label)));
        // Will return all instances of active tasks for a jobKey. After a task is killed for an update, it transitions
        // into a PENDING state as part of same transaction. So if we dont have any active tasks, then we are OK.
        LOG.debug("Number of found tasks " + Iterables.size(foundTasks));
        LOG.debug("Found tasks" + foundTasks.toString());
        if (Iterables.isEmpty(foundTasks)) {
          LOG.debug("Attempting to unreserve resource" + offer.getOffer().getId() + "for label " +
              label.toString());
          offerManager.unReserveOffer(offer.getOffer().getId(), Collections.singletonList(resource));
          break;
        }
      }
    }
  }
}
