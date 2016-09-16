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
package org.apache.aurora.scheduler.events;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.google.common.base.MoreObjects;

import com.google.common.collect.ImmutableMap;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Defines configuration for Webhook.
 */
public class WebhookInfo {
  private static final Logger LOG = LoggerFactory.getLogger(WebhookInfo.class);

  private final Integer connectTimeout;
  private final Map<String, String> headers;
  private final String targetURL;

  /**
   * Return key:value pairs of headers to set for every connection.
   *
   * @return Map
   */
  public Map<String, String> getHeaders() {
    return this.headers;
  }

  /**
   * Returns URI where to post events.
   *
   * @return URI
   */
  URI getTargetURI() {
    URI targetURI = null;
    try {
      targetURI = new URI(targetURL);
    } catch (URISyntaxException exp) {
      LOG.error("Error setting URI for Webhook URL", exp);
    }
    return targetURI;
  }

  /**
   * Returns connection timeout to set when POSTing an event.
   *
   * @return Integer value.
   */
  Integer getConnectonTimeout() {
    return this.connectTimeout;
  }

  @JsonCreator
  public WebhookInfo(
       @JsonProperty("headers") Map<String, String> headers,
       @JsonProperty("targetURL") String targetURL,
       @JsonProperty("timeoutMsec") Integer timeout) {

    this.headers = ImmutableMap.copyOf(headers);
    this.targetURL = requireNonNull(targetURL);
    this.connectTimeout = requireNonNull(timeout);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("headers", headers.toString())
      .add("targetURL", targetURL)
      .add("connectTimeout", connectTimeout)
      .toString();
  }
}
