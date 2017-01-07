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
package org.apache.aurora.scheduler.offers;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;

/**
 * Tracks the Offers currently known by the scheduler.
 */
public interface OfferManager extends EventSubscriber {

  // add offerAdded type, OfferReconciliation imports offerAdded type.

  void unReserveOffer(OfferID offerId, List<Protos.Resource> reservedResourceList);

  /**
   * Reserve the offer on behalf of the scheduler.
   *
   * @param offer Matched offer.
   * @param task Matched task info.
   */
  void reserveAndLaunchTask(HostOffer offer, IAssignedTask task);

  void launchAndReserveTask(HostOffer offer, IAssignedTask iAssignedTask) throws LaunchException;

//  void removeTaskId(String taskId);


//  HashSet<String> getReservedTasks();


  /**
   * Notifies the scheduler of a new resource offer.
   *
   * @param offer Newly-available resource offer.
   */
  void addOffer(HostOffer offer);

  /**
   * Invalidates an offer.  This indicates that the scheduler should not attempt to match any
   * tasks against the offer.
   *
   * @param offerId Canceled offer.
   */
  void cancelOffer(OfferID offerId);

  /**
   * Exclude an offer that results in a static mismatch from further attempts to match against all
   * tasks from the same group.
   *
   * @param offerId Offer ID to exclude for the given {@code groupKey}.
   * @param groupKey Task group key to exclude.
   */
  void banOffer(OfferID offerId, TaskGroupKey groupKey);

  /**
   * Launches the task matched against the offer.
   *
   * @param offer Matched offer.
   * @param task Matched task info.
   * @throws LaunchException If there was an error launching the task.
   */
  void launchTask(Protos.Offer offer, IAssignedTask task) throws LaunchException;

  /**
   * Notifies the offer queue that a host's attributes have changed.
   *
   * @param change State change notification.
   */
  void hostAttributesChanged(HostAttributesChanged change);

  /**
   * Gets the offers that the scheduler is holding.
   *
   * @return A snapshot of the offers that the scheduler is currently holding.
   */
  Iterable<HostOffer> getOffers();

  /**
   * Gets all offers that are not statically banned for the given {@code groupKey}.
   *
   * @param groupKey Task group key to check offers for.
   * @return A snapshot of all offers eligible for the given {@code groupKey}.
   */
  Iterable<HostOffer> getOffers(TaskGroupKey groupKey);

  /**
   * Gets an offer for the given slave ID.
   *
   * @param slaveId Slave ID to get offer for.
   * @return An offer for the slave ID.
   */
  Optional<HostOffer> getOffer(SlaveID slaveId);

  /**
   * Thrown when there was an unexpected failure trying to launch a task.
   */
  class LaunchException extends Exception {
    @VisibleForTesting
    public LaunchException(String msg) {
      super(msg);
    }

    LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  class OfferManagerImpl implements OfferManager {
    @VisibleForTesting
    static final Logger LOG = LoggerFactory.getLogger(OfferManagerImpl.class);
    @VisibleForTesting
    static final String OFFER_ACCEPT_RACES = "offer_accept_races";
    @VisibleForTesting
    static final String OUTSTANDING_OFFERS = "outstanding_offers";
    @VisibleForTesting
    static final String STATICALLY_BANNED_OFFERS = "statically_banned_offers_size";

    private final HostOffers hostOffers;
    private final AtomicLong offerRaces;

    private final Driver driver;
    private final OfferSettings offerSettings;
    private final DelayExecutor executor;
    private final StatsProvider statsProvider;
    private final MesosTaskFactory taskFactory;
//    private final Storage storage;
    private final DriverSettings driverSettings;
//    private final ReservationStore.Mutable reservationStore;
//    public HashSet<String> reservedTasks;

    @Inject
    @VisibleForTesting
    public OfferManagerImpl(
        Driver driver,
        OfferSettings offerSettings,
        StatsProvider statsProvider,
        MesosTaskFactory taskFactory,
        @AsyncExecutor DelayExecutor executor,
//        Storage storage,
        DriverSettings driverSettings
//        ReservationStore.Mutable reservationStore,
//        EventSink eventSink
    ) {

      this.driver = requireNonNull(driver);
      this.offerSettings = requireNonNull(offerSettings);
      this.executor = requireNonNull(executor);
      this.offerRaces = statsProvider.makeCounter(OFFER_ACCEPT_RACES);
      this.statsProvider = requireNonNull(statsProvider);
      this.taskFactory = requireNonNull(taskFactory);
      this.hostOffers = new HostOffers(statsProvider);
//      this.storage = storage;
//      this.eventSink = requireNonNull(eventSink);
      this.driverSettings = requireNonNull(driverSettings);
//      this.reservationStore = requireNonNull(reservationStore);
//      this.reservedTasks = new HashSet<String>();
    }

//    public HashSet<String> getReservedTasks() {
//      return this.reservedTasks;
//    }

//    public void removeTaskId(String taskId) {
//      this.reservedTasks.remove(taskId);
//    }

    @Override
    public void reserveAndLaunchTask(HostOffer offer, IAssignedTask iAssignedTask) {
      // This will sort our resources in the correct order and try to use the ones that have been reserved first.
      Protos.TaskInfo task = taskFactory.createFrom(iAssignedTask, offer.getOffer());
      OfferID offerId = offer.getOffer().getId();
      LOG.info("Task name is " + task.getName());
      String taskNameInstanceId = task.getName() + "/" + iAssignedTask.getInstanceId();
      List<Protos.Resource> resourcesList = task.getResourcesList();
      // TODO: need driverSettings to set role here.
      List<Protos.Resource> reservedResourceList = new ArrayList<>();
      for (Protos.Resource resource: resourcesList) {
        List<Protos.Label> labels = new ArrayList<>();
        labels.add(Protos.Label.newBuilder()
            .setKey("task_name")
            .setValue(taskNameInstanceId)
              .build()
        );
        reservedResourceList.add(
            Protos.Resource.newBuilder(resource)
                // TODO: set role taken by the framework
//                DriverSettings
//                .setRole("aurora-role")
                .setRole(driverSettings.getFrameworkInfo().getRole())
                .setReservation(Protos.Resource.ReservationInfo.newBuilder()
//                    .setPrincipal("aurora")
                    // TODO: what if principal isn't set
                    .setPrincipal(driverSettings.getFrameworkInfo().getPrincipal())
                    .setLabels(Protos.Labels.newBuilder()
                        .addAllLabels(labels).build())
                )
            .build()
        );
      }
      Protos.TaskInfo newTask = task.toBuilder().clearResources().addAllResources(reservedResourceList).build();

      Operation reserve = Protos.Offer.Operation.newBuilder()
          .setType(Operation.Type.RESERVE)
          .setReserve(
              Protos.Offer.Operation.Reserve.newBuilder().addAllResources(reservedResourceList)
          )
          .build();
      Operation launch = Protos.Offer.Operation.newBuilder()
          .setType(Protos.Offer.Operation.Type.LAUNCH)
          .setLaunch(Protos.Offer.Operation.Launch.newBuilder()
              .addTaskInfos(newTask))
          .build();
//  Placeholder for AURORA-181? which will add ReservationStore
//        storage.write(
//            (Storage.MutateWork.NoResult.Quiet) storeProvider ->
//              storeProvider.getReservationStore().saveReserervedTasks(taskNameInstanceId));

//      reservationStore.saveReserervedTasks(taskNameInstanceId);
//      this.reservedTasks.add(taskNameInstanceId);

      List<Operation> operations = Arrays.asList(reserve, launch);
      LOG.info("Finished building operation");
      driver.acceptOffers(offerId, operations, getOfferFilter());
    }

    @Override
    public void addOffer(final HostOffer offer) {
      // We run a slight risk of a race here, which is acceptable.  The worst case is that we
      // temporarily hold two offers for the same host, which should be corrected when we return
      // them after the return delay.
      // There's also a chance that we return an offer for compaction ~simultaneously with the
      // same-host offer being canceled/returned.  This is also fine.
      Optional<HostOffer> sameSlave = hostOffers.get(offer.getOffer().getSlaveId());
      if (sameSlave.isPresent()) {
        // If there are existing offers for the slave, decline all of them so the master can
        // compact all of those offers into a single offer and send them back.
        LOG.info("Returning offers for " + offer.getOffer().getSlaveId().getValue()
            + " for compaction.");
        decline(offer.getOffer().getId());
        removeAndDecline(sameSlave.get().getOffer().getId());
      } else {
        hostOffers.add(offer);
        executor.execute(
            () -> removeAndDecline(offer.getOffer().getId()),
            offerSettings.getOfferReturnDelay());
      }
    }

    void removeAndDecline(OfferID id) {
      if (removeFromHostOffers(id)) {
        decline(id);
      }
    }

    void decline(OfferID id) {
      LOG.debug("Declining offer {}", id);
      driver.declineOffer(id, getOfferFilter());
    }

    private Protos.Filters getOfferFilter() {
      return Protos.Filters.newBuilder()
          .setRefuseSeconds(offerSettings.getOfferFilterDuration().as(Time.SECONDS))
          .build();
    }

    @Override
    public void cancelOffer(final OfferID offerId) {
      removeFromHostOffers(offerId);
    }

    private boolean removeFromHostOffers(final OfferID offerId) {
      requireNonNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      return hostOffers.remove(offerId);
    }

    @Override
    public Iterable<HostOffer> getOffers() {
      return hostOffers.getOffers();
    }

    @Override
    public Iterable<HostOffer> getOffers(TaskGroupKey groupKey) {
      return hostOffers.getWeaklyConsistentOffers(groupKey);
    }

    @Override
    public Optional<HostOffer> getOffer(SlaveID slaveId) {
      return hostOffers.get(slaveId);
    }

    /**
     * Updates the preference of a host's offers.
     *
     * @param change Host change notification.
     */
    @Subscribe
    public void hostAttributesChanged(HostAttributesChanged change) {
      hostOffers.updateHostAttributes(change.getAttributes());
    }

    /**
     * Notifies the queue that the driver is disconnected, and all the stored offers are now
     * invalid.
     * <p>
     * The queue takes this as a signal to flush its queue.
     *
     * @param event Disconnected event.
     */
    @Subscribe
    public void driverDisconnected(DriverDisconnected event) {
      LOG.info("Clearing stale offers since the driver is disconnected.");
      hostOffers.clear();
    }

    /**
     * A container for the data structures used by this class, to make it easier to reason about
     * the different indices used and their consistency.
     */
    private static class HostOffers {
      private static final Comparator<HostOffer> PREFERENCE_COMPARATOR =
          // Currently, the only preference is based on host maintenance status.
          Ordering.explicit(NONE, SCHEDULED, DRAINING, DRAINED)
              .onResultOf(new Function<HostOffer, MaintenanceMode>() {
                @Override
                public MaintenanceMode apply(HostOffer offer) {
                  return offer.getAttributes().getMode();
                }
              })
              .compound(Ordering.arbitrary());

      private final Set<HostOffer> offers = new ConcurrentSkipListSet<>(PREFERENCE_COMPARATOR);
      private final Map<OfferID, HostOffer> offersById = Maps.newHashMap();
      private final Map<SlaveID, HostOffer> offersBySlave = Maps.newHashMap();
      private final Map<String, HostOffer> offersByHost = Maps.newHashMap();
      // TODO(maxim): Expose via a debug endpoint. AURORA-1136.
      // Keep track of offer->groupKey mappings that will never be matched to avoid redundant
      // scheduling attempts. See VetoGroup for more details on static ban.
      private final Multimap<OfferID, TaskGroupKey> staticallyBannedOffers = HashMultimap.create();

      HostOffers(StatsProvider statsProvider) {
        // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more expensive.
        // Could track this separately if it turns out to pose problems.
        statsProvider.exportSize(OUTSTANDING_OFFERS, offers);
        statsProvider.makeGauge(STATICALLY_BANNED_OFFERS, () -> staticallyBannedOffers.size());
      }

      synchronized Optional<HostOffer> get(SlaveID slaveId) {
        return Optional.fromNullable(offersBySlave.get(slaveId));
      }

      synchronized void add(HostOffer offer) {
        offers.add(offer);
        offersById.put(offer.getOffer().getId(), offer);
        offersBySlave.put(offer.getOffer().getSlaveId(), offer);
        offersByHost.put(offer.getOffer().getHostname(), offer);
      }

      synchronized boolean remove(OfferID id) {
        HostOffer removed = offersById.remove(id);
        if (removed != null) {
          offers.remove(removed);
          offersBySlave.remove(removed.getOffer().getSlaveId());
          offersByHost.remove(removed.getOffer().getHostname());
          staticallyBannedOffers.removeAll(id);
        }
        return removed != null;
      }

      synchronized void updateHostAttributes(IHostAttributes attributes) {
        HostOffer offer = offersByHost.remove(attributes.getHost());
        if (offer != null) {
          // Remove and re-add a host's offer to re-sort based on its new hostStatus
          remove(offer.getOffer().getId());
          add(new HostOffer(offer.getOffer(), attributes));
        }
      }

      synchronized Iterable<HostOffer> getOffers() {
        return ImmutableSet.copyOf(offers);
      }

      synchronized Iterable<HostOffer> getWeaklyConsistentOffers(TaskGroupKey groupKey) {
        return Iterables.unmodifiableIterable(FluentIterable.from(offers).filter(
            e -> !staticallyBannedOffers.containsEntry(e.getOffer().getId(), groupKey)));
      }

      synchronized void addStaticGroupBan(OfferID offerId, TaskGroupKey groupKey) {
        if (offersById.containsKey(offerId)) {
          staticallyBannedOffers.put(offerId, groupKey);
        }
      }

      synchronized void clear() {
        offers.clear();
        offersById.clear();
        offersBySlave.clear();
        offersByHost.clear();
        staticallyBannedOffers.clear();
      }
    }

    @Override
    public void banOffer(OfferID offerId, TaskGroupKey groupKey) {
      hostOffers.addStaticGroupBan(offerId, groupKey);
    }

    @Timed("offer_manager_launch_task")
    @Override
    public void launchAndReserveTask(HostOffer offer, IAssignedTask iAssignedTask) throws LaunchException {
      // Guard against an offer being removed after we grabbed it from the iterator.
      // If that happens, the offer will not exist in hostOffers, and we can immediately
      // send it back to LOST for quick reschedule.
      // Removing while iterating counts on the use of a weakly-consistent iterator being used,
      // which is a feature of ConcurrentSkipListSet.
//      Protos.TaskInfo task = taskFactory.createFrom(iAssignedTask, offer.getOffer());
      OfferID offerId = offer.getOffer().getId();
      if (hostOffers.remove(offerId)) {
        try {

          // See if task requires dynamic reservation or it's fine as is. If it does want a dynamic reservation
          // then just use driver.current class, reserveAndLaunchTask() method.

          reserveAndLaunchTask(offer, iAssignedTask);
        } catch (IllegalStateException e) {
          // TODO(William Farner): Catch only the checked exception produced by Driver
          // once it changes from throwing IllegalStateException when the driver is not yet
          // registered.
          throw new LaunchException("Failed to launch task.", e);
        }
      } else {
        offerRaces.incrementAndGet();
        throw new LaunchException("Offer no longer exists in offer queue, likely data race.");
      }
    }


    @Timed("offer_manager_launch_task")
    @Override
    public void launchTask(Protos.Offer offer, IAssignedTask iAssignedTask) throws LaunchException {
      // Guard against an offer being removed after we grabbed it from the iterator.
      // If that happens, the offer will not exist in hostOffers, and we can immediately
      // send it back to LOST for quick reschedule.
      // Removing while iterating counts on the use of a weakly-consistent iterator being used,
      // which is a feature of ConcurrentSkipListSet.
      OfferID offerId = offer.getId();
      Protos.TaskInfo task = taskFactory.createFrom(iAssignedTask, offer);
      if (hostOffers.remove(offerId)) {
        try {
          Operation launch = Operation.newBuilder()
              .setType(Operation.Type.LAUNCH)
              .setLaunch(Operation.Launch.newBuilder().addTaskInfos(task))
              .build();
          driver.acceptOffers(offerId, ImmutableList.of(launch), getOfferFilter());
        } catch (IllegalStateException e) {
          // TODO(William Farner): Catch only the checked exception produced by Driver
          // once it changes from throwing IllegalStateException when the driver is not yet
          // registered.
          throw new LaunchException("Failed to launch task.", e);
        }
      } else {
        offerRaces.incrementAndGet();
        throw new LaunchException("Offer no longer exists in offer queue, likely data race.");
      }
    }
  }
}
