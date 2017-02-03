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
package org.apache.aurora.scheduler.base;

import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

import com.google.common.base.Splitter;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.mesos.Protos.TaskInfo;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility function for {@link IInstanceKey instance keys}.
 */
public final class InstanceKeys {
  private InstanceKeys() {
    // Utility class.
  }

  /**
   * Creates an instance key from a job and instance ID.
   *
   * @param job Job key.
   * @param instanceId Instance id.
   * @return Instance ID.
   */
  public static IInstanceKey from(IJobKey job, int instanceId) {
    Objects.requireNonNull(job);
    Preconditions.checkArgument(instanceId >= 0);
    return IInstanceKey.build(new InstanceKey(job.newBuilder(), instanceId));
  }

  /**
   * Creates an instance key from a job and instance ID.
   *
   * @param taskInfo TaskInfo object.
   * @param iAssignedTask AssignedTask containing instanceId.
   * @return InstanceKey object.
   */
  public static IInstanceKey from(TaskInfo taskInfo, IAssignedTask iAssignedTask) {
    Objects.requireNonNull(taskInfo);
    Objects.requireNonNull(iAssignedTask);

    return InstanceKeys.from(JobKeys.parse(taskInfo.getName()), iAssignedTask.getInstanceId());
  }

  public static IInstanceKey parse(String string) throws IllegalArgumentException {
    List<String> components = Splitter.on("/").splitToList(string);
    checkArgument(components.size() == 4);
    return from(
        JobKeys.from(components.get(0), components.get(1), components.get(2)),
        Integer.parseInt(components.get(3))
    );
  }

  /**
   * Creates a human-friendly string for an instance key.
   *
   * @param instance Instance key.
   * @return String representation of the instance key.
   */
  public static String toString(IInstanceKey instance) {
    return JobKeys.canonicalString(instance.getJobKey()) + "/" + instance.getInstanceId();
  }
}
