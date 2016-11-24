package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.*;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.ReservationStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;


//TODO: add code that will occasionally kick off and reconcile number of tasks with number of reserved offers

public class OfferReconciler implements PubsubEvent.EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(OfferReconciler.class);

  private final OfferManager offerManager;
//  private final ReservationStore reservationStore;
  private final Storage storage;

  @Inject
  @VisibleForTesting
  public OfferReconciler(
      OfferManager offerManager,
//      ReservationStore reservationStore,
      Storage storage)
  {
    this.offerManager = offerManager;
//    this.reservationStore = reservationStore;
    this.storage = storage;
  }

  @Subscribe
  public void offerAdded(PubsubEvent.OfferAdded offerAdded) {
    // Works by looking for a task_name included as part of a resource of an offer TaskInfo.getName()
    // and then trying to find a task in active state in the store. If none are found, then
    // unreserve the offer.
    HostOffer offer = offerAdded.getOffer();
    List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
    //TODO: we should only unreserve resource that we reserved, not the entire offer. Is this true?
    boolean unreserve = false;



//    LOG.info("Inside offerAdded callback with state of: " + offerManager.getReservedTasks().toString());


    //TODO: put back
    LOG.info("Inside offerAdded callback with state of: " + storage.read(storeProvider -> storeProvider.getReservationStore().fetchReservedTasks()));

    String task_name = "";
    // How do we know that this is our our offer?

    for (Protos.Resource resource: resourceList) {
      Protos.Resource.ReservationInfo resInfo = resource.getReservation();
      Protos.Labels labels = resInfo.getLabels();
      for (Protos.Label label: labels.getLabelsList()) {
        // TODO: how to loop through two labels
        task_name = label.getValue();
        LOG.info(("Found task_name " + task_name));
        // task_name is gonna be of 'role/env/job_name/0' format.


        LOG.info("task_name: " + task_name);

        List<String> parsed = Splitter.on("/").splitToList(task_name);
        IJobKey jobKey2 = JobKeys.from(parsed.get(0), parsed.get(1), parsed.get(2));
        int instanceId = Integer.parseInt(parsed.get(3));

        //TODO: scope down to all in transition state but not RUNNING, since RUNNING means that
        // it could be an extra offer with changed resources.

        Iterable<IScheduledTask> foundTasks = storage.read(
            storeProvider -> storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(jobKey2, instanceId).activeNotRunning()));

        // Will return all instances of active tasks for a jobKey. After a task is killed for an update, it transitions
        // into a PENDING state as part of same transaction. So if we dont have any active tasks, then we are OK.

        LOG.info("Number of found tasks " + Iterables.size(foundTasks));
        LOG.info("Found tasks" + foundTasks.toString());

        // Right now we are unreserving if we find no active tasks matching the label on the offer.
        // But what if the offer does not match anymore? What if we have more dynamic reservations than we have running
        // tasks. If we made it here, then it's possible that either a task is getting launched OR we no longer need it.

        // TODO: OR if we have unneeded reservations eg after we did resource reshaping or decreased instance count
        if (Iterables.isEmpty(foundTasks)) {
          unreserve = true;
          break;
        }
      }
      // We unreserve inside this for loop because we dont want to unreserve resources we did not reserve.
      if (unreserve) {
        // TODO: Here we are unreserving one resource at a time. We should batch them and do them all at once.
        LOG.info("Attempting to unreserve resource" + offer.getOffer().getId() + "for task " + task_name);
        this.offerManager.unReserveOffer(offer.getOffer().getId(), Arrays.asList(resource));
        break;
      }
    }
  }
}
