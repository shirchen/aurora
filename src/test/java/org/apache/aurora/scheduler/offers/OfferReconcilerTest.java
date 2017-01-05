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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.mesos.Protos.Value.Type.SCALAR;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;


public class OfferReconcilerTest extends EasyMockTest {
  private static final Logger LOG = LoggerFactory.getLogger(OfferReconcilerTest.class);

  private static final HostOffer OFFER =
      new HostOffer(Protos.Offer.getDefaultInstance(), IHostAttributes.build(new HostAttributes()));
  private final OfferAdded offerAddedToKeep = new OfferAdded(OFFER);

  private static final String FRAMEWORK_ID = "framework-id";
  private static final Protos.FrameworkID FRAMEWORK =
      Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
  private static final String SLAVE_HOST = "slave-hostname";
  private static final Protos.SlaveID SLAVE_ID = Protos.SlaveID.newBuilder().setValue("slave-id").build();

  private static final Protos.OfferID OFFER_ID = Protos.OfferID.newBuilder().setValue("offer-id").build();

  private static final Protos.Label LABEL = Protos.Label.newBuilder()
      .setKey("task_name").setValue("foo/bar/buz/0").build();

  private static Protos.Resource MEM_RESOURCE = Protos.Resource.newBuilder()
      .setName("mem")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(128.0).build())
      .setReservation(
          Protos.Resource.ReservationInfo.newBuilder().setLabels(
              Protos.Labels.newBuilder().addLabels(LABEL).build()))
      .build();

  private static Protos.Resource CPU_RESOURCE = Protos.Resource.newBuilder()
      .setName("cpus")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(1).build())
      .setReservation(
          Protos.Resource.ReservationInfo.newBuilder().setLabels(
              Protos.Labels.newBuilder().addLabels(LABEL).build()))
      .build();

  private static final HostOffer OFFER_WITH_RESERVED = new HostOffer(
      Protos.Offer.newBuilder()
          .setFrameworkId(FRAMEWORK)
          .setSlaveId(SLAVE_ID)
          .setHostname(SLAVE_HOST)
          .setId(OFFER_ID)
          .addResources(
              Protos.Resource.newBuilder().setType(SCALAR).setName(CPUS.getMesosName())
                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(2.0))
          )
          .addResources(MEM_RESOURCE)
          .addResources(CPU_RESOURCE)
          .addResources(mesosScalar(DISK_MB, 2048))
          .build(),
      IHostAttributes.build(
          new HostAttributes()
              .setHost(SLAVE_HOST)
              .setSlaveId(SLAVE_ID.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));
  private static final IScheduledTask TASK_A =
      TaskTestUtil.makeTask("a", JobKeys.from("a", "a", "a"));


  private final OfferAdded offerAddedToUnreserve = new OfferAdded(OFFER_WITH_RESERVED);


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
    Query.Builder queryBuilder = OfferReconciler.buildQuery(LABEL);
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of());
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of());

    offerManager.unReserveOffer(OFFER_WITH_RESERVED.getOffer().getId(), Collections.singletonList(MEM_RESOURCE));
    expectLastCall();
    offerManager.unReserveOffer(OFFER_WITH_RESERVED.getOffer().getId(), Collections.singletonList(CPU_RESOURCE));
    expectLastCall();
    control.replay();
    // Replay mode
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }

  @Test
  public void testReservedResourcesTasksFoundAndRunningTasks() throws Exception {
    // Since there is a task in transition, we should not unreserve it's resources.

    // Record mode
    Query.Builder queryBuilder = OfferReconciler.buildQuery(LABEL);
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of(TASK_A));
    storageUtil.expectTaskFetch(queryBuilder, ImmutableSet.of(TASK_A));
    control.replay();
    // Replay mode
    offerReconciler.offerAdded(offerAddedToUnreserve);
  }
}


