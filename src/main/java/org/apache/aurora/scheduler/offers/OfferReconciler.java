package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.OfferAdded;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;


/**
 * A pubsub event subscriber that accepts an OfferAdded message and unreserves previously
 * dynamically reserved resources that are no longer needed.
 * Logic is as follows:
 *  We loop over the entire list of resources as part of that offer since not all resources may need
 *  to be unreserved.
 *  The we try to match the label which is a task name in form of 'role/env/job_name/0' to an
 * existing task that is in an active state but is not running.
 *  Why? We want to only unreserve resources part of an offer that came back and which tasks are not
 *  in process of being scheduled. We could get resources that come back after we have either killed
 *  a task or did some resource shaping: either decreased or increased necessary resources for a
 *  task.
 */
public class OfferReconciler implements EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(OfferReconciler.class);

  private final OfferManager offerManager;
  private final Storage storage;

  @Inject
  @VisibleForTesting
  public OfferReconciler(OfferManager offerManager, Storage storage)
  {
    this.offerManager = offerManager;
    this.storage = storage;
  }


  @Subscribe
  public void offerAdded(OfferAdded offerAdded) {
    HostOffer offer = offerAdded.getOffer();
    if (offer.hasReserved()) {
      List<Resource> resourceList = offer.getOffer().getResourcesList();
      for (Resource resource : resourceList) {
        Resource.ReservationInfo resInfo = resource.getReservation();
        Labels labels = resInfo.getLabels();
        // Label list should only have one label, but this is defensive code.
        for (Label label : labels.getLabelsList()) {
          if (label.getKey().equals("instance_key")) {
            // if label's value is not properly formatted, then an exception will get thrown.
            IInstanceKey instanceKey = InstanceKeys.parse(label.getValue());

            Iterable<IScheduledTask> foundTasks = storage.read(
                storeProvider -> storeProvider
                    .getTaskStore()
                    .fetchTasks(
                        Query.instanceScoped(instanceKey)
                            .byStatus(ScheduleStatus.RUNNING)));
            // If there is a matching task that is running, then we do not risk getting stuck in a
            // state where an offer is still making it's way through the scheduling code, so we can
            // unreserve it. A task cannot be in multiple scheduling states and label is unique.
            LOG.debug("Found running tasks" + foundTasks.toString());
            if (!Iterables.isEmpty(foundTasks)) {
              LOG.debug("Attempting to unreserve resource" + offer.getOffer().getId() + "for label " + label.toString());
              offerManager.unReserveOffer(offer.getOffer().getId(), Collections.singletonList(resource));
              break;
            }
          }
        }
      }
    }
  }
}
