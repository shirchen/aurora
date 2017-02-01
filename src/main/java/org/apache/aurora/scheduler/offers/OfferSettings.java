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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;

import static java.util.Objects.requireNonNull;

/**
 * Settings required to create an OfferManager.
 */
@VisibleForTesting
public class OfferSettings {

  private final Amount<Long, Time> offerFilterDuration;
  private final Amount<Long, Time> reservedOfferWait;
  private final Supplier<Amount<Long, Time>> returnDelaySupplier;

  public OfferSettings(Amount<Long, Time> offerFilterDuration, Amount<Long, Time> reservedOfferWait,
                       Supplier<Amount<Long, Time>> returnDelaySupplier) {

    this.offerFilterDuration = requireNonNull(offerFilterDuration);
    this.reservedOfferWait = requireNonNull(reservedOfferWait);
    this.returnDelaySupplier = requireNonNull(returnDelaySupplier);
  }

  /**
   * Duration after which we want Mesos to re-offer unused or declined resources.
   */
  public Amount<Long, Time> getOfferFilterDuration() {
    return offerFilterDuration;
  }

  /**
   * Maximum time to wait for an offer with labels for reserved resources to come back.
   */
  public Amount<Long, Time> getReservedOfferWait() {
    return reservedOfferWait;
  }

  /**
   * The amount of time after which an unused offer should be 'returned' to Mesos by declining it.
   * The delay is calculated for each offer using a random duration within a fixed window.
   */
  public Amount<Long, Time> getOfferReturnDelay() {
    return returnDelaySupplier.get();
  }
}
