package org.apache.aurora.scheduler.offers;


import com.google.common.collect.ImmutableSet;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.OfferAdded;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
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
        InstanceKeys.parse(HostOffers.LABEL.getValue()),
        IInstanceKey.build(
            new InstanceKey(
                JobKeys.from("foo", "bar", "buz").newBuilder(), 0))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLabel() throws Exception {
    control.replay();
    InstanceKeys.parse("foo/bar/buz");
  }

  @Test
  public void testReservedResourcesTasksFoundAndNoRunningTasks() throws Exception {
    // Do not UNRESERVE this matching InstanceKey is not in RUNNING state.
    storageUtil.expectTaskFetch(
        Query.instanceScoped(InstanceKeys.parse(HostOffers.LABEL.getValue()))
            .byStatus(ScheduleStatus.RUNNING),
        ImmutableSet.of());
    expectLastCall().times(2);
    control.replay();

    OfferAdded offerAddedToUnreserve = new OfferAdded(OFFER_WITH_RESERVED);
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }

  @Test
  public void testReservedResourcesTasksFoundAndRunningTasks() throws Exception {
    // 2 resources should be unreserved because matching task is in RUNNING state.
    storageUtil.expectTaskFetch(
        Query.instanceScoped(InstanceKeys.parse(HostOffers.LABEL.getValue()))
            .byStatus(ScheduleStatus.RUNNING),
        ImmutableSet.of(TASK_A));
    expectLastCall().times(2);

    offerManager.unReserveOffer(
        OFFER_WITH_RESERVED.getOffer().getId(),
        Collections.singletonList(HostOffers.makeCPUResource(HostOffers.LABEL)));
    expectLastCall();
    offerManager.unReserveOffer(
        OFFER_WITH_RESERVED.getOffer().getId(),
        Collections.singletonList(HostOffers.makeMemResource(HostOffers.LABEL)));
    expectLastCall();


    control.replay();

    OfferAdded offerAddedToUnreserve = new OfferAdded(OFFER_WITH_RESERVED);
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }
}


