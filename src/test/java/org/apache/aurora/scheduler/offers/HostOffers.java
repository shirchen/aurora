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

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos;

import static org.apache.aurora.gen.MaintenanceMode.NONE;

public class HostOffers {
  /**
   * Utility class for creating host resource offers in unit tests.
   */
    private HostOffers() {
      // Utility class.
    }
  public static final Protos.Label LABEL = Protos.Label.newBuilder()
      .setKey("instance_key").setValue("foo/bar/buz/0").build();

  public static Protos.Resource makeCPUResource(Protos.Label label) {
      return Protos.Resource.newBuilder()
        .setName("cpus")
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(1.0).build())
        .setReservation(Protos.Resource.ReservationInfo.newBuilder()
            .setLabels(Protos.Labels.newBuilder()
                .addLabels(label))
            .build())
        .build();
  }

  public static Protos.Resource makeMemResource(Protos.Label label) {

    return Protos.Resource.newBuilder().setName("mem")
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(128.0).build())
        .setReservation(Protos.Resource.ReservationInfo.newBuilder()
            .setLabels(Protos.Labels.newBuilder()
                .addLabels(label))
            .build())
        .build();
  }

  public static final Protos.Resource DISK_RESOURCE = Protos.Resource.newBuilder()
      .setName("disk")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(128.0).build())
      .setReservation(Protos.Resource.ReservationInfo.newBuilder()
          .setLabels(Protos.Labels.newBuilder()
              .addLabels(Protos.Label.newBuilder()
                  .setKey("key"))
              .build())
          .build())
      .setDisk(Protos.Resource.DiskInfo.newBuilder()
          .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
              .setId("volume")
              .build())
          .setVolume(Protos.Volume.newBuilder()
              .setContainerPath("path")
              .setMode(Protos.Volume.Mode.RW)
              .setImage(Protos.Image.newBuilder()
                  .setType(Protos.Image.Type.DOCKER)
                  .setDocker(Protos.Image.Docker.newBuilder()
                      .setName("image")
                      .build())
                  .build())
              .build())
          .setSource(Protos.Resource.DiskInfo.Source.newBuilder()
              .setType(Protos.Resource.DiskInfo.Source.Type.PATH)
              .setPath(Protos.Resource.DiskInfo.Source.Path.newBuilder()
                  .setRoot("root")
                  .build())
              .build())
          .build())
      .build();

  public static final Protos.Resource GPU_RESOURCE = Protos.Resource.newBuilder()
      .setName("gpus")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(4.0).build())
      .build();

  public static final Protos.Resource PORTS_RESOURCE = Protos.Resource.newBuilder()
      .setName("ports")
      .setType(Protos.Value.Type.RANGES)
      .setRanges(Protos.Value.Ranges.newBuilder()
          .addRange(Protos.Value.Range.newBuilder()
              .setBegin(31000)
              .setEnd(32000)
              .build())
          .build())
      .build();

  public static HostOffer makeHostOffer() {
    return HostOffers.makeHostOffer(
        Protos.Label.newBuilder().setKey("key").setValue("value").build()
    );
  }

  public static HostOffer makeHostOffer(Protos.Label label) {
    return new HostOffer(
        Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("offer_id"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework_id"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave_id"))
            .setHostname("host_name")
            .addResources(makeCPUResource(label))
            .addResources(makeMemResource(label))
            .addResources(DISK_RESOURCE)
            .addResources(GPU_RESOURCE)
            .addResources(PORTS_RESOURCE)
            .build(),
        IHostAttributes.build(new HostAttributes().setMode(NONE)));
  }
}
