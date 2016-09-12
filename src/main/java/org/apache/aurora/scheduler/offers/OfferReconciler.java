package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;


//TODO: add code that will occasionally kick off and reconcile number of tasks with number of reserved offers

public class OfferReconciler implements PubsubEvent.EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(OfferReconciler.class);

  private final OfferManager offerManager;
  private final Storage storage;

  @Inject
  @VisibleForTesting
  public OfferReconciler(
      OfferManager offerManager,
      Storage storage)
  {
    this.offerManager = offerManager;
    this.storage = storage;
  }


  private boolean isTaskActive(IScheduledTask foundTask) {
    ScheduleStatus status = foundTask.getStatus();
    ImmutableList<ITaskEvent> events = foundTask.getTaskEvents();
    ITaskEvent rescheduleEvent = events.get(events.size() - 1);

    boolean isActive;
    isActive = rescheduleEvent.getMessage().equals("Killed for job update.") || Tasks.ACTIVE_STATES.contains(status);

    LOG.info("status is " + status.toString() + "events are " + events.toString());
    return isActive;
  }

  @Subscribe
  public void offerAdded(PubsubEvent.OfferAdded offerAdded) {
    HostOffer offer = offerAdded.getOffer();
    LOG.info("Looking at offer " + offer.toString());
    List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
    //TODO: we should only unreserve resource that we reserved, not the entire offer.
    boolean unreserve = false;

    for (Protos.Resource resource: resourceList) {
      Protos.Resource.ReservationInfo resInfo = resource.getReservation();
      Protos.Labels labels = resInfo.getLabels();
      for (Protos.Label label: labels.getLabelsList()) {
        // TODO: how to loop through two labels
        String task_name = label.getValue();
        LOG.info(("Found task_name " + task_name));
        // task_name is gonna be of 'role/env/job_name' format.

        IJobKey jobKey = JobKeys.parse(task_name);
        // Will return all instances of active tasks for a jobKey. After a task is killed for an update, it transitions
        // into a PENDING state as part of same transaction. So if we dont have any active tasks, then we are OK.

        Iterable<IScheduledTask> foundTasks = storage.read(
            storeProvider -> storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()));

        LOG.info("Number of found tasks " + Iterables.size(foundTasks));
        LOG.info("List of active tasks " + Iterables.filter(foundTasks, new Predicate<IScheduledTask>() {
          @Override
          public boolean apply(@Nullable IScheduledTask input) {
            return Tasks.ACTIVE_STATES.contains(input.getStatus());
          }
        }));
        // Right now we are unreserving if we find no active tasks matching the label on the offer.
        // But what if the offer does not match anymore? What if we have more dynamic reservations than we have running
        // tasks. If we made it here, then it's possible that either a task is getting launched OR we no longer need it.
        if (Iterables.isEmpty(foundTasks)) {
          unreserve = true;
          break;
        }
      }
      if (unreserve) {
        this.offerManager.unReserveOffer(offer.getOffer().getId(), Arrays.asList(resource));
      }
    }
  }
}
