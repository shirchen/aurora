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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

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
   * @param taskId Task id to assign.
   * @param slaveReservations Slave reservations.
   * @return Assignment result.
   */
  boolean maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      String taskId,
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

    private TaskInfo assign(
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
      return taskFactory.createFrom(assigned, offer);
    }

    @Timed("assigner_maybe_assign")
    @Override
    public boolean maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        String taskId,
        Map<String, TaskGroupKey> slaveReservations) {
      LOG.info("TaskID is" + taskId);
      // TODO: figure out how to look for an existing offer
      LOG.info("Looking at TaskGroupKey " + groupKey);
      for (HostOffer offer : offerManager.getOffers(groupKey)) {
        Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
            slaveReservations.get(offer.getOffer().getSlaveId().getValue()));
        LOG.info("Looking at offer inside TaskAssigner" + offer.toString());

        if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
          // This slave is reserved for a different task group -> skip.
          continue;
        }

        List<Protos.Resource> resourceList = offer.getOffer().getResourcesList();
        for (Protos.Resource resource : resourceList) {
          Protos.Resource.ReservationInfo resInfo = resource.getReservation();
          Protos.Labels labels = resInfo.getLabels();
          List<Protos.Label> labelList = labels.getLabelsList();
          for (Protos.Label label : labelList) {
            String labelValue = label.getValue();
            String taskName = JobKeys.canonicalString(resourceRequest.getTask().getJob());
            LOG.info("Task name extracted from task about to be scheduled is " +
                taskName + "while label is " + labelValue);
            if (labelValue.equals(taskName)) {
              // Then we found our reservation!!!!
              LOG.info("Found matching offer for " + groupKey.toString());
            }
            //TODO: if labelValue matches then we found the winner!

          }
        }


        TierInfo tierInfo = tierManager.getTier(groupKey.getTask());
        Set<Veto> vetoes = filter.filter(
            new UnusedResource(offer.getResourceBag(tierInfo), offer.getAttributes()),
            resourceRequest);

        if (vetoes.isEmpty()) {
          TaskInfo taskInfo = assign(
              storeProvider,
              offer.getOffer(),
              taskId);

          // TODO: try to reserve the offer if task requires us to do so


          try {
//            Protos.OfferID offerID = offer.getOffer().getId();
            // TODO: see if we already reserved an offer.

            // Logic: have groupKey -> [offer_id_0, offer_id_1,....]
            // try to see if groupKey exists in the MultiMap inside offerManager.
            // if yes, then try to see if this offerId matches. if it has already been used then
            //    let's reuse it again.
            // if not, then this is a brand new task (or non-reserved) so just try to launch it.

//            Collection<Protos.OfferID> groupKeysForOffer = offerManager.getDynamic(groupKey);
//            if (groupKeysForOffer.contains(offerID)) {
//              // If we found a previous offerId in our hashed list so we should try to reuse this offer
//              offerManager.launchTask(offer.getOffer().getId(), taskInfo);
//              return true;
////            } else if () {
////              // Case where this job has never been dynamically reserved.
//
//            } else {
//              offerManager.addToDynamic(groupKey, offerID);
//              offerManager.launchTask(offer.getOffer().getId(), taskInfo);
//              return true;
//            }

            offerManager.launchTask(offer.getOffer().getId(), taskInfo);
            return true;
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
            return false;
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
      return false;
    }
  }
}
