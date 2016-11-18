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
package org.apache.aurora.scheduler.storage;

import java.util.HashSet;

/**
 * Point of storage for tasks that have to be dynamically reserved.
 */
public interface ReservationStore {

  /**
   * Fetches all reserved tasks.
   *
   * @return All reserved tasks.
   */
  HashSet<String> fetchReservedTasks();

  interface Mutable extends ReservationStore {
    /**
     * Saves taskId as one needing a dynamic reservation.
     *
     * @param taskId taskId to add as one no longer needing to wait for a dynamic reservation.
     */
    void saveReserervedTasks(String taskId);

    /**
     * Removes a taskId from list of ones needing a dynamic reservation.
     *
     * @param taskId taskId to remove as one no longer needing to wait for a dynamic reservation.
     */
    void removeTaskId(String taskId);
  }
}
