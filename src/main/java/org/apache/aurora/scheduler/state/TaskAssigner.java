/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.state;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.google.common.collect.Maps;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;

/**
 * Responsible for matching a task against an offer and launching it.
 */
public interface TaskAssigner {
  /**
   * Tries to match a task against an offer.  If a match is found, the assigner makes the
   * appropriate changes to the task and requests task launch.
   *
   * @param storeProvider Storage provider.
   * @param resourceRequest The request for resources being scheduled.
   * @param groupKey Task group key.
   * @param taskIds Task IDs to assign.
   * @param slaveReservations Slave reservations.
   * @return Successfully assigned task IDs.
   */
  Set<String> maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<String> taskIds,
      Map<String, TaskGroupKey> slaveReservations);

  class TaskAssignerImpl implements TaskAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignerImpl.class);

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    private final AtomicLong launchFailures = Stats.exportLong("assigner_launch_failures");

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final MesosTaskFactory taskFactory;
    private final OfferManager offerManager;
    private final TierManager tierManager;
    private final Map<String, Integer> taskIdTostart = Maps.newHashMap();

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        MesosTaskFactory taskFactory,
        OfferManager offerManager,
        TierManager tierManager) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.taskFactory = requireNonNull(taskFactory);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);
    }

    @VisibleForTesting
    IAssignedTask mapAndAssignResources(Offer offer, IAssignedTask task) {
      IAssignedTask assigned = task;
      for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
        if (type.getMapper().isPresent()) {
          assigned = type.getMapper().get().mapAndAssign(offer, assigned);
        }
      }
      return assigned;
    }

    private IAssignedTask assign(
        MutableStoreProvider storeProvider,
        Offer offer,
        String taskId) {

      String host = offer.getHostname();
      IAssignedTask assigned = stateManager.assignTask(
          storeProvider,
          taskId,
          host,
          offer.getSlaveId(),
          task -> mapAndAssignResources(offer, task));
      LOG.info(
          "Offer on agent {} (id {}) is being assigned task for {}.",
          host, offer.getSlaveId().getValue(), taskId);
      return assigned;
    }

    private boolean waitedLongEnough(String taskId) {
      boolean enoughWaiting = false;
      Integer startedToWaitAt = this.taskIdTostart.get(taskId);
      long timeMillis = System.currentTimeMillis();
      long timeSeconds = TimeUnit.MILLISECONDS.toSeconds(timeMillis);
      Integer timeNow =  (int)timeSeconds;
      LOG.info("Waited " + (timeNow - startedToWaitAt) + "secs");
      LOG.info("Map:" + this.taskIdTostart);
      // If waited for more than 1 minute then just reserve the damn thing.
      if (timeNow > startedToWaitAt + 60*1) {
        enoughWaiting = true;
      }

      return enoughWaiting;
    }

    private boolean isReservedOfferForTask(ResourceRequest resourceRequest, HostOffer offer, int instanceId) {
      boolean found = false;
        // skip unless we find our offer
//        LOG.info("Skipping offer because we are looking to launch a reserved task");
        List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
        for (Protos.Resource resource: resourceList) {
          Protos.Resource.ReservationInfo resInfo = resource.getReservation();
          Protos.Labels labels = resInfo.getLabels();
          List<Protos.Label> labelList = labels.getLabelsList();
          for (Protos.Label label : labelList) {
            String labelValue = label.getValue();
            String taskName = JobKeys.canonicalString(resourceRequest.getTask().getJob()) + "/" + instanceId;
            LOG.info("Task name extracted from task about to be scheduled is " + taskName + " while label is " + labelValue);
            if (labelValue.equals(taskName)) {
              LOG.info("Found our reservation");
              found = true;
              break;
            }
          }
        }
      return found;
    }

    @Timed("assigner_maybe_assign")
    @Override
    public Set<String> maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        Iterable<String> taskIds,
        Map<String, TaskGroupKey> slaveReservations) {

      if (Iterables.isEmpty(taskIds)) {
        return ImmutableSet.of();
      }

      TierInfo tierInfo = tierManager.getTier(groupKey.getTask());
      ImmutableSet.Builder<String> assignmentResult = ImmutableSet.builder();
      Iterator<String> remainingTasks = taskIds.iterator();
      String taskId = remainingTasks.next();

//      LOG.info("TaskID is" + taskId);
      // TODO: figure out how to look for an existing offer
//      LOG.info("Looking at TaskGroupKey " + groupKey);
      for (HostOffer offer : offerManager.getOffers(groupKey)) {

        LOG.info("List of all offers we are trying to find for " + groupKey + "is: " + offerManager.getOffers(groupKey));


        Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(slaveReservations.get(offer.getOffer().getSlaveId().getValue()));
//        LOG.info("Looking at offer inside TaskAssigner" + offer.toString());
        boolean found = false;
        if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
          // This slave is reserved for a different task group -> skip.
          continue;
        }

        Set<Veto> vetoes = filter.filter(new UnusedResource(offer.getResourceBag(tierInfo), offer.getAttributes()), resourceRequest);
        LOG.info("Recording vetoes: " + vetoes.toString());


        String taskName2 = JobKeys.canonicalString(resourceRequest.getTask().getJob());
        LOG.info("Trying to find " + taskName2 + " in this offer");

//        boolean newTask = false;
//        Optional<IScheduledTask> scheduledTask = storeProvider.getTaskStore().fetchTask(taskId);
//        if (scheduledTask.isPresent()) {
//          // TODO: try to figure out if there was a killed/scheduled event in the past.
//          LOG.info("List of events for this task" + scheduledTask.get().getTaskEvents().toString());
////          scheduledTask.get().getAssignedTask()
//          if (scheduledTask.get().getTaskEvents().size() == 1) {
//            newTask = true;
//          }
//        }
        // TODO: see how to tell if we are launching for the first time. Then we need to just launch
        // tasks.


        // TODO: look for an existing active task?


        Optional<IScheduledTask> scheduledTask = storeProvider.getTaskStore().fetchTask(taskId);
        if (scheduledTask.isPresent()) {
          IAssignedTask assignedTask = scheduledTask.get().getAssignedTask();
//          LOG.info("Found our task in the store: " + assignedTask.toString());
          String taskName3 = taskName2 + "/" + assignedTask.getInstanceId();

          LOG.info("looking at task " + taskName3);
          LOG.info("State of reserved tasks " + offerManager.getReservedTasks().toString());
          if (offerManager.getReservedTasks().contains(taskName3)) {
            LOG.info("Found task inside reserved list so it must be dynamically reserved " + taskName3);
            long timeMillis = System.currentTimeMillis();
            long timeSeconds = TimeUnit.MILLISECONDS.toSeconds(timeMillis);
            LOG.info("Before put: " + this.taskIdTostart.toString());
            this.taskIdTostart.putIfAbsent(taskId, (int) timeSeconds);
            LOG.info("After put: " + this.taskIdTostart.toString());
            LOG.info("current timer mapping " + this.taskIdTostart.toString());
            // We must look for reserved offer and skip non-reserved ones.
            found = isReservedOfferForTask(resourceRequest, offer, assignedTask.getInstanceId());
            LOG.info("Whether offer is reserved for this task: " + found);
            //TODO: create a timer within which we must find our offer.
            if (!found && !waitedLongEnough(taskId)) {
              // We did not find our offer nor waited long enough so continue looking for it.
              LOG.info("Skipping offer because we are looking to launch a reserved task and did not"
                  + " wait long enough. We can tell because the found flag is" + found);
              continue;
            }
          }
        }

        if (vetoes.isEmpty()) {
//          TaskInfo taskInfo = assign(
          IAssignedTask assignedTask = assign(
              storeProvider,
              offer.getOffer(),
              taskId);


          resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());

          try {
            this.taskIdTostart.remove(taskId);
            if (found) {
              // Just need to perform launch operation.
              LOG.info("Found the offer and launching task");
              offerManager.launchTask(offer, assignedTask);
//              return true;
            } else {
              LOG.info("Either did not find offer or launch for the first time");
              // Need to perform reserve + launch operation.
              offerManager.launchAndReserveTask(offer, assignedTask);

//              return true;
            }

            assignmentResult.add(taskId);

            if (remainingTasks.hasNext()) {
              taskId = remainingTasks.next();
            } else {
              break;
            }



          } catch (OfferManager.LaunchException e) {
            LOG.warn("Failed to launch task.", e);
            launchFailures.incrementAndGet();

            // The attempt to schedule the task failed, so we need to backpedal on the
            // assignment.
            // It is in the LOST state and a new task will move to PENDING to replace it.
            // Should the state change fail due to storage issues, that's okay.  The task will
            // time out in the ASSIGNED state and be moved to LOST.
            stateManager.changeState(
                storeProvider,
                taskId,
                Optional.of(PENDING),
                LOST,
                LAUNCH_FAILED_MSG);
            break;
          }
        } else {
          if (Veto.identifyGroup(vetoes) == VetoGroup.STATIC) {
            // Never attempt to match this offer/groupKey pair again.
            offerManager.banOffer(offer.getOffer().getId(), groupKey);
          }
          LOG.debug("Agent " + offer.getOffer().getHostname()
              + " vetoed task " + taskId + ": " + vetoes);
        }
      }

      return assignmentResult.build();
    }
  }
}
