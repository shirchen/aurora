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
import org.apache.aurora.common.stats.StatsProvider;
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
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
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
    @VisibleForTesting
    static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";
    @VisibleForTesting
    static final String ASSIGNER_EVALUATED_OFFERS = "assigner_evaluated_offers";

    private final AtomicLong launchFailures;
    private final AtomicLong evaluatedOffers;

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final OfferManager offerManager;
    private final TierManager tierManager;
    private final StoreProvider storeProvider;
    private final Map<String, Integer> taskIdToStartTime = Maps.newHashMap();

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        OfferManager offerManager,
        TierManager tierManager,
        StatsProvider statsProvider,
        StoreProvider storeProvider) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);

      this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
      this.evaluatedOffers = statsProvider.makeCounter(ASSIGNER_EVALUATED_OFFERS);
      this.storeProvider = requireNonNull(storeProvider);
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
      Integer startedToWaitAt = this.taskIdToStartTime.get(taskId);

      long timeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      Integer timeNow =  (int)timeSeconds;
//      LOG.info("Waited " + (timeNow - startedToWaitAt) + "secs");
//      LOG.info("Map:" + this.taskIdToStartTime);
      // TODO: Wire this up as an optional argument.
      if (timeNow > startedToWaitAt + 60*1) {
        enoughWaiting = true;
      }
      return enoughWaiting;
    }

    private boolean hasReservedResourcesInsideOfferForTask(
        ResourceRequest resourceRequest, HostOffer offer, int instanceId) {
      boolean found = false;
      List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
      for (Protos.Resource resource : resourceList) {
        Protos.Resource.ReservationInfo resInfo = resource.getReservation();
        Protos.Labels labels = resInfo.getLabels();
        List<Protos.Label> labelList = labels.getLabelsList();
        for (Protos.Label label : labelList) {
          String labelValue = label.getValue();
          String taskName = JobKeys.canonicalString(
              resourceRequest.getTask().getJob()) + "/" + instanceId;
          LOG.info(
              "Task name extracted from task about to be scheduled is " + taskName + " while label is " + labelValue);
          if (labelValue.equals(taskName)) {
            LOG.info("Found our reservation");
            found = true;
            break;
          }
        }
      }
      return found;
    }

//    private boolean reallySkipOffer(TierInfo tierInfo) {
//      boolean skip = false;
//      if (tierInfo.isReserved()) {
//        //placeholder: see if task exists inside reservation store. assuming that yes until we
//        // implement that part.
//        skip = true;
//      } else {
//        skip = false;
//      }
//     return skip;
//    }
//
    // Mindfully commented out to be optionally put in later.
//    private boolean isInsideReservationStore(IAssignedTask assignedTask, String taskName) {
//    boolean alreadyReserved  = false;
//    String taskName3 = taskName + "/" + assignedTask.getInstanceId();
//    if (storeProvider.getReservationStore().fetchReservedTasks().contains(taskName3)) {
//      alreadyReserved = true;
//     }
//
//      return alreadyReserved;
//    }

//    private boolean runningAsReserved(ResourceRequest resourceRequest, String taskId) {
//      boolean alreadyRunning = false;
//      String taskName2 = JobKeys.canonicalString(resourceRequest.getTask().getJob());
//      Optional<IScheduledTask> scheduledTask = storeProvider.getTaskStore().fetchTask(taskId);
//
//      if (scheduledTask.isPresent()) {
//        IAssignedTask assignedTask = scheduledTask.get().getAssignedTask();
//        if (isInsideReservationStore(assignedTask, taskName2)) {
//          alreadyRunning = true;
//        }
//      }
//
//      return alreadyRunning;
//    }

    private void startTiming(String taskId) {
      long timeMillis = System.currentTimeMillis();
      long timeSeconds = TimeUnit.MILLISECONDS.toSeconds(timeMillis);
      this.taskIdToStartTime.putIfAbsent(taskId, (int) timeSeconds);
    }

    private boolean skipThisOffer(TierInfo tierInfo, ResourceRequest resourceRequest, String taskId,
                                  HostOffer offer) {
      boolean skip = false;

      if (tierInfo.isReserved()) {
        Optional<IScheduledTask> scheduledTask = storeProvider.getTaskStore().fetchTask(taskId);
        if (scheduledTask.isPresent()) {
          IAssignedTask assignedTask = scheduledTask.get().getAssignedTask();
          startTiming(taskId);
          // We must look for reserved offer and skip non-reserved ones.
          boolean found = hasReservedResourcesInsideOfferForTask(resourceRequest, offer, assignedTask.getInstanceId());
          if (!found && !waitedLongEnough(taskId)) {
            // We did not find our offer nor waited long enough so continue looking for it.
            skip = true;
          }
        }
      }
      // If skip is still false then we did not find our reserved resources or did not wait long
      // enough.

      return skip;
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

      for (HostOffer offer : offerManager.getOffers(groupKey)) {
        evaluatedOffers.incrementAndGet();

        Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
            slaveReservations.get(offer.getOffer().getSlaveId().getValue()));

        if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
          // This slave is reserved for a different task group -> skip.
          continue;
        }

        Set<Veto> vetoes = filter.filter(
            new UnusedResource(offer.getResourceBag(tierInfo), offer.getAttributes()), resourceRequest);

        // Need to differentiate b/n an Offer that just needs to launch a task versus one that
        // we need to reserve resources for. Is this true?
        if (skipThisOffer(tierInfo, resourceRequest, taskId, offer)) {
          // Skipping because we require a dynamic reservation and this offer doesn't match our reqs.
          continue;
        }

        boolean launchWithReserved = false;
        // Launch a reserved task with found resources.
        if (tierInfo.isReserved()) {
          Optional<IScheduledTask> scheduledTask = storeProvider.getTaskStore().fetchTask(taskId);
          if (scheduledTask.isPresent()) {
            IAssignedTask assignedTask = scheduledTask.get().getAssignedTask();
            launchWithReserved = hasReservedResourcesInsideOfferForTask(
                resourceRequest, offer, assignedTask.getInstanceId());
          }
        }

        if (vetoes.isEmpty()) {
          IAssignedTask assignedTask = assign(
              storeProvider,
              offer.getOffer(),
              taskId);

          resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());

          try {
            if (tierInfo.isReserved()) {
              LOG.info("tierInfo is RESERVED: " + tierInfo.toString());
              this.taskIdToStartTime.remove(taskId);
              if (launchWithReserved) {
                // Just need to perform launch operation.
                LOG.info("Found the offer and launching task");
                offerManager.launchTask(offer.getOffer(), assignedTask, false);
              } else {
                LOG.info("Either did not find offer or launch for the first time");
                // Need to perform reserve + launch operation.
                offerManager.launchTask(offer.getOffer(), assignedTask, true);
              }
            } else {
              // Task does not need reserved resources.
              offerManager.launchTask(offer.getOffer(), assignedTask, false);
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
          LOG.debug("Agent {} vetoed task {}: {}", offer.getOffer().getHostname(), taskId, vetoes);
        }
      }

      return assignmentResult.build();
    }
  }
}
