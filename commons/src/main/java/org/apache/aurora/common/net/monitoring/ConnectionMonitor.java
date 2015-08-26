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
package org.apache.aurora.common.net.monitoring;

/**
 * Monitors active connections between two hosts..
 *
 * @author William Farner
 */
public interface ConnectionMonitor<K> {

  /**
   * Instructs the monitor that a connection was established.
   *
   * @param connectionKey Key for the host that a connection was established with.
   */
  public void connected(K connectionKey);

  /**
   * Informs the monitor that a connection was released.
   *
   * @param connectionKey Key for the host that a connection was released for.
   */
  public void released(K connectionKey);
}