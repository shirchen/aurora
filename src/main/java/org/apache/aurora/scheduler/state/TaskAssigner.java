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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.Query;
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
   * @param assignableTaskMap Task IDs to IAssignedTask mapping.
   * @param slaveReservations Slave reservations.
   * @return Successfully assigned task IDs.
   */
  Set<String> maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Map<String, IAssignedTask> assignableTaskMap,
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
    private final MesosTaskFactory taskFactory;
    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final OfferManager offerManager;
    private final TierManager tierManager;
    private final Clock clock;

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        OfferManager offerManager,
        TierManager tierManager,
        MesosTaskFactory taskFactory,
        StatsProvider statsProvider,
        Clock clock) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);
      this.taskFactory = taskFactory;
      this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
      this.evaluatedOffers = statsProvider.makeCounter(ASSIGNER_EVALUATED_OFFERS);
      this.clock = clock;
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

    @VisibleForTesting
    boolean waitedLongEnough(String taskId, MutableStoreProvider storeProvider) {
      // We want to find the last PENDING event and see how long we have been waiting for an offer
      // to come back.
      Iterable<IScheduledTask> scheduledTasks = FluentIterable.from(
          storeProvider
              .getTaskStore()
              .fetchTasks(Query.taskScoped(taskId).byStatus(ScheduleStatus.PENDING)));

      long timestamp = Iterables.getLast(
          Iterables.getLast(scheduledTasks).getTaskEvents()).getTimestamp();
      LOG.debug("Waited " + (clock.nowMillis() - timestamp) + " before launching " + taskId);
      return (clock.nowMillis() >
          timestamp + offerManager.getReservedOfferWait().as(Time.MILLISECONDS));
    }

    @Timed("assigner_maybe_assign")
    @Override
    public Set<String> maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        Map<String, IAssignedTask> assignableTaskMap,
        Map<String, TaskGroupKey> slaveReservations) {

      Iterable<String> taskIds = assignableTaskMap.keySet();

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
        boolean launchAnyway = false;
        boolean newReserved = false;
        Set<Veto> vetoes = filter.filter(
            new UnusedResource(
                offer.getResourceBag(tierInfo), offer.getAttributes()),
            resourceRequest
        );
        LOG.info("Vetoes before we switch" + vetoes);
        if (tierInfo.isReserved()) {
          // IF we waited long enough, this may be a new task or one for which matching offer can
          // not be found.
          if (waitedLongEnough(taskId, storeProvider)) {
            java.util.Optional<Veto> returned = vetoes.stream().filter(veto -> {
              if (veto.getVetoType().equals(SchedulingFilter.VetoType.INSUFFICIENT_RESOURCES)) {
                return false;
              } else {
                return true;
              }
            }).findAny();
            // Then we see which ones of the vetoes are resource related and whether we can clear
            // them by switching tiers.
            if (!returned.isPresent() && tierInfo.isReserved()) {
              vetoes = filter.filter(new UnusedResource(offer.getResourceBag(tierInfo.unReserve()), offer.getAttributes()), resourceRequest);
              LOG.info("Vetoes after we switch tiers " + vetoes);
              tierInfo.reReserve();
              // So now we know that the offer is big enough and we should be able to launch with it.
            }
            launchAnyway = true;
            newReserved = true;
          }
          //TODO: see if we still need to do the logic below if we have logic above ^.
          if (vetoes.isEmpty()) {
            IAssignedTask iAssignedTask = assignableTaskMap.get(taskId);
            // Right now we are not getting a veto for a reserved task not finding the correct offer.
            // Now if we launch for the first time we need to ignore that one.
            vetoes = filter.filterForReserved(
                new UnusedResource(
                    offer.getResourceBag(tierInfo),
                    offer.getAttributes(),
                    offer.getOffer().getResourcesList()),
                new SchedulingFilter.SpecificResourceRequest(
                    resourceRequest, iAssignedTask.getInstanceId()));
            if (vetoes.equals(Stream.of(Veto.reservation()).collect(Collectors.toSet()))) {
              // iff we get one one reservation veto then we check when last PENDING event happened.
              // If task has been in PENDING waiting for offer to come back, then we launch task on
              // first non-vetoed offer. Otherwise, we keep on waiting.
              launchAnyway = waitedLongEnough(taskId, storeProvider);
              // if waitedLongEnough and we got vetoes on resources then we need to stop

            }
          }
        }

        if (vetoes.isEmpty() || launchAnyway) {
          IAssignedTask assignedTask = assign(
              storeProvider,
              offer.getOffer(),
              taskId);

          resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());
          Protos.TaskInfo taskInfo = taskFactory.createFrom(assignedTask, offer.getOffer(), newReserved);
          try {
            if (tierInfo.isReserved() && launchAnyway) {
              LOG.info("Either did not find matching offer or launching task for the first time");
              offerManager.launchTask(offer.getOffer(), taskInfo, true);
            } else {
              offerManager.launchTask(offer.getOffer(), taskInfo, false);
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
          if (Veto.identifyGroup(vetoes) == VetoGroup.STATIC
              && ! vetoes.equals(Stream.of(Veto.reservation()).collect(Collectors.toSet()))) {
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
