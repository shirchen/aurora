package org.apache.aurora.scheduler.offers;


import com.google.common.collect.ImmutableSet;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.OfferAdded;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskQuery;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;


public class OfferReconcilerTest extends EasyMockTest {
  private static final HostOffer OFFER =
      new HostOffer(Protos.Offer.getDefaultInstance(), IHostAttributes.build(new HostAttributes()));
  private final OfferAdded offerAddedToKeep = new OfferAdded(OFFER);

  private static final HostOffer OFFER_WITH_RESERVED = HostOffers.makeHostOffer(HostOffers.LABEL);
  private static final IScheduledTask TASK_A =
      TaskTestUtil.makeTask("a", JobKeys.from("a", "a", "a"));

  private OfferManager offerManager;
  private StorageTestUtil storageUtil;
  private OfferReconciler offerReconciler;

  @Before
  public void setUp() {
    offerManager = createMock(OfferManager.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    offerReconciler = new OfferReconciler(offerManager, storageUtil.storage);
  }

  @Test
  public void testValidLabel() {
    control.replay();
    assertEquals(
        OfferReconciler.isValidLabel(HostOffers.LABEL), true);
    // No instance_id
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("foo/bar/buz").build()),
            false);
    // Not a num
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("foo/bar/buz/bur").build()),
        false);
    // Short job_key
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("foo/bar/0").build()),
        false);
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("////").build()),
        false);
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("///").build()),
        false);
    assertEquals(
        OfferReconciler.isValidLabel(
            Protos.Label.newBuilder().setKey("task_name").setValue("///0").build()),
        false);

  }

  @Test
  public void testBuildQuery() {
    control.replay();
    Protos.Label label = Protos.Label.newBuilder().setKey("task_name").setValue("role/env/job_name/0").build();
    Query.Builder foo = OfferReconciler.buildQuery(label);
    TaskQuery query = new TaskQuery().setRole("role").setEnvironment("env").setJobName("job_name").setTaskIds(null).
        setInstanceIds(ImmutableSet.of(0)).
        setStatuses(new HashSet<>(Arrays.asList(
            ScheduleStatus.RESTARTING, ScheduleStatus.PREEMPTING, ScheduleStatus.ASSIGNED,
            ScheduleStatus.STARTING, ScheduleStatus.KILLING, ScheduleStatus.THROTTLED,
            ScheduleStatus.DRAINING, ScheduleStatus.PENDING)))
        .setSlaveHosts(null).setJobKeys(null).setOffset(0).setLimit(0);
    assertEquals(foo.get(), ITaskQuery.build(query));
  }

  @Test
  public void testNoReservedResourcesTasksFound() {
    // Record mode
    control.replay();
    // Replay mode
    offerReconciler.offerAdded(offerAddedToKeep);
  }

  @Test
  public void testReservedResourcesTasksFoundAndNoRunningTasks() throws Exception {
    // 2 resources should be unreserved because they have no active non running tasks.
    // Record mode
    Query.Builder queryBuilder = OfferReconciler.buildQuery(HostOffers.LABEL);
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of());
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of());

    offerManager.unReserveOffer(
        OFFER_WITH_RESERVED.getOffer().getId(),
        Collections.singletonList(HostOffers.makeMemResource(HostOffers.LABEL)));
    expectLastCall();
    offerManager.unReserveOffer(
        OFFER_WITH_RESERVED.getOffer().getId(),
        Collections.singletonList(HostOffers.makeCPUResource(HostOffers.LABEL)));
    expectLastCall();
    control.replay();
    // Replay mode
    OfferAdded offerAddedToUnreserve = new OfferAdded(OFFER_WITH_RESERVED);
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }

  @Test
  public void testReservedResourcesTasksFoundAndRunningTasks() throws Exception {
    // Since there is a task in transition, we should not unreserve it's resources.

    // Record mode
    Query.Builder queryBuilder = OfferReconciler.buildQuery(HostOffers.LABEL);
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of(TASK_A));
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of(TASK_A));
    control.replay();
    // Replay mode
    OfferAdded offerAddedToUnreserve = new OfferAdded(OFFER_WITH_RESERVED);
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }
}


