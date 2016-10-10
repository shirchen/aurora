package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.*;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
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
    // Works by looking for a task_name included as part of a resource of an offer TaskInfo.getName()
    // and then trying to find a task in active state in the store. If none are found, then
    // unreserve the offer.
    HostOffer offer = offerAdded.getOffer();
    List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
    //TODO: we should only unreserve resource that we reserved, not the entire offer. Is this true?
    boolean unreserve = false;

    for (Protos.Resource resource: resourceList) {
      Protos.Resource.ReservationInfo resInfo = resource.getReservation();
      Protos.Labels labels = resInfo.getLabels();
      for (Protos.Label label: labels.getLabelsList()) {
        // TODO: how to loop through two labels
        String task_name = label.getValue();
        LOG.info(("Found task_name " + task_name));
        // task_name is gonna be of 'role/env/job_name/0' format.


        String jobKey = task_name;

        LOG.info("jobKey" + jobKey);

//        String instanceId = task_name.substring(task_name.lastIndexOf('/') + 1);
//        LOG.info("instanceId " + instanceId);

        List<String> parsed = Splitter.on("/").splitToList(task_name);
        IJobKey jobKey2 = JobKeys.from(parsed.get(0), parsed.get(1), parsed.get(2));
        int instanceId = Integer.parseInt(parsed.get(3));
//        String instance_id = parsed[parsed.size()-1];
//        String job_key = parsed;
//        InstanceKeys.from(task_name, instance_id);
//        storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(task_name)


//        Query.instanceScoped(jobKey2, instanceId);


        //TODO: scope down to all in transition state but not RUNNING, since RUNNING means that
        // it could be an extra offer with changed resources.
        Iterable<IScheduledTask> foundTasks = storage.read(
            storeProvider -> storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(jobKey2, instanceId).activeNotRunning()));


//        IJobKey jobKey = JobKeys.parse(task_name);


        // Will return all instances of active tasks for a jobKey. After a task is killed for an update, it transitions
        // into a PENDING state as part of same transaction. So if we dont have any active tasks, then we are OK.

        //TODO: but what if we have running tasks but we requested a new offer. Then what?

        // TODO: SEE how many tasks we should actually be running.
//        Iterable<IScheduledTask> foundTasks = storage.read(
//            storeProvider -> storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()));

        // Filter out any tasks in RUNNING state
//        Iterable<IScheduledTask> allTasks = storage.read(
//            storeProvider -> storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey)));
//        LOG.info("ALL tasks found ever:" + allTasks);

        LOG.info("Number of found tasks " + Iterables.size(foundTasks));
        LOG.info("Found tasks" + foundTasks.toString());

//        LOG.info("List of active tasks " + Iterables.filter(foundTasks, new Predicate<IScheduledTask>() {
//          @Override
//          public boolean apply(@Nullable IScheduledTask input) {
//            return Tasks.ACTIVE_STATES.contains(input.getStatus());
//          }
//        }));
        // Right now we are unreserving if we find no active tasks matching the label on the offer.
        // But what if the offer does not match anymore? What if we have more dynamic reservations than we have running
        // tasks. If we made it here, then it's possible that either a task is getting launched OR we no longer need it.

        // TODO: OR if we have unneeded reservations eg after we did resource reshaping or decreased instance count
        if (Iterables.isEmpty(foundTasks)) {
          unreserve = true;
          break;
        }
      }
      if (unreserve) {
        // TODO: Here we are unreserving one resource at a time. We should batch them and do them all at once.
        this.offerManager.unReserveOffer(offer.getOffer().getId(), Arrays.asList(resource));
      }
    }
  }
}
