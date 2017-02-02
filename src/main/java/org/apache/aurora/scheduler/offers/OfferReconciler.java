package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.OfferAdded;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;


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
public class OfferReconciler implements EventSubscriber {

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

  public static boolean isValidLabel(Protos.Label label) {
    // TODO: change to parseLabel and return it.
    
    // Helper to check whether a label can potentially generate a task_name that we can pull from
    // TaskStore.
    boolean valid = false;

    // TODO: move this logic into InstanceKeys.

    // Change label's key from task_name to "instance_key" add to OfferManager

    String keyLabel = label.getKey();
    String valueLabel = label.getValue();
    // We expect label to be in an InstanceKeys.
    if (keyLabel.equals("task_name")
        && (Splitter.on("/").splitToList(valueLabel).size() == 4)
        && StringUtils.isNumeric(Splitter.on("/").splitToList(valueLabel).get(3))
        && ! Splitter.on("/").splitToList(valueLabel).contains("")
        ) {
      valid = true;
    }
    return valid;
  }

  public static Query.Builder buildQuery(Protos.Label label) {
    // Now all tasks that are not in progress will be in PENDING, ASSIGNED, etc, but NOT
    // RUNNING. So if a task is in RUNNING, then unreserve. Otherwise, we may need this offer.
    // task_name is gonna an InstanceKeys.

    // TODO: move into InstanceKeys: fromString
    String taskName = label.getValue();
    List<String> parsed = Splitter.on("/").splitToList(taskName);
    IJobKey jobKey = JobKeys.from(parsed.get(0), parsed.get(1), parsed.get(2));
    // Assuming that instance_id will always be set to an int inside OfferManager.
    int instanceId = Integer.parseInt(parsed.get(3));
    return Query.instanceScoped(jobKey, instanceId).activeNotRunning();
  }

  @Subscribe
  public void offerAdded(OfferAdded offerAdded) {
    // TODO: filter here


    HostOffer offer = offerAdded.getOffer();
    List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
    for (Protos.Resource resource: resourceList) {
      Protos.Resource.ReservationInfo resInfo = resource.getReservation();
      Protos.Labels labels = resInfo.getLabels();
      // Label list should only have one label, but this is defensive code.
       // Throw an exception if the label is unparsable can use Preconditions.checkState
      // if (label.name == "instance_key"), and unparsable value, throw exception via checkState
      for (Protos.Label label: labels.getLabelsList()) {
        if (isValidLabel(label)) {
          // Get all tasks that are Active but not RUNNING.


          // Invert the logic and scope down to just finding RUNNING tasks and unreserving those.
          Iterable<IScheduledTask> foundTasks = storage.read(storeProvider -> storeProvider.getTaskStore().fetchTasks(OfferReconciler.buildQuery(label)));
          // Will return all instances of active tasks for a jobKey. After a task is killed for an update, it transitions
          // into a PENDING state as part of same transaction. So if we dont have any active tasks, then we are OK.
          LOG.debug("Found tasks" + foundTasks.toString());
          if (Iterables.isEmpty(foundTasks)) {
            LOG.debug("Attempting to unreserve resource" + offer.getOffer().getId() + "for label " + label.toString());
            offerManager.unReserveOffer(offer.getOffer().getId(), Collections.singletonList(resource));
            break;
          }
        }
      }
    }
  }
}
