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
package org.apache.aurora.scheduler;

import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Defines common task tier traits and behaviors.
 */
public final class TierInfo {

  private final boolean preemptible;
  private final boolean revocable;
  private boolean reserved;

  @JsonCreator
  public TierInfo(
      @JsonProperty("preemptible") boolean preemptible,
      @JsonProperty("revocable") boolean revocable,
      @JsonProperty("reserved") boolean reserved
  ) {

    this.preemptible = preemptible;
    this.revocable = revocable;
    this.reserved = reserved;
  }

  /**
   * Checks if this tier intends to run tasks as preemptible.
   *
   * @return {@code true} if this tier will result in tasks being run as preemptible, {@code false}
   * otherwise.
   */
  public boolean isPreemptible() {
    return preemptible;
  }

  /**
   * Checks if this tier intends to run with Mesos revocable resource offers.
   *
   * @return {@code true} if this tier requires revocable resource offers, {@code false} otherwise.
   */
  public boolean isRevocable() {
    return revocable;
  }


  /**
   * Checks if this tier intends to run with Mesos revocable resource offers.
   *
   * @return {@code true} if this tier requires reserved resource offers, {@code false} otherwise.
   */
  public boolean isReserved() {
    return reserved;
  }

  /**
   * Used by scheduling filter to try and find an offer for an unreserved tier when launching
   * a task requiring dynamic reservations for the first time.
   */
  public TierInfo unReserve() {
    reserved = false;
    return this;
  }

  /**
   * Flip reserved flag again.
   */
  public TierInfo reReserve() {
    reserved = true;
    return this;
  }

  /**
   * Gets the map of tier attribute names to values.
   *
   * @return A readonly view of all tier attributes.
   */
  public Map<String, String> toMap() {
    return ImmutableMap.of(
        "preemptible", String.valueOf(preemptible),
        "revocable", String.valueOf(revocable),
        "reserved", String.valueOf(reserved)
    );
  }

  @Override
  public int hashCode() {
    return Objects.hash(preemptible, revocable, reserved);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TierInfo)) {
      return false;
    }

    TierInfo other = (TierInfo) obj;
    return Objects.equals(preemptible, other.preemptible)
        && Objects.equals(revocable, other.revocable)
        && Objects.equals(reserved, other.reserved);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("preemptible", preemptible)
        .add("revocable", revocable)
        .add("reserved", reserved)
        .toString();
  }
}
