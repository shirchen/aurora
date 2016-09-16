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


import org.apache.aurora.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WebhookTest extends EasyMockTest {

  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", TaskTestUtil.JOB);
  private final TaskStateChange change = TaskStateChange.initialized(TASK);
  private final TaskStateChange changeWithOldState = TaskStateChange
      .transition(TASK, ScheduleStatus.FAILED);
  private final String changeJson = changeWithOldState.toJson();


  private WebhookInfo webhookInfo;
  private HttpClient httpClient;
  private Webhook webhook;


  @Before
  public void setUp() {
    webhookInfo = WebhookModule.parseWebhookConfig(WebhookModule.readWebhookFile());
    httpClient = createMock(HttpClient.class);
    webhook = new Webhook(httpClient, webhookInfo);
  }

  @Test
  public void testTaskChangedStateNoOldState() throws Exception {
    // Should be a noop as oldState is MIA so this test would have throw an exception.
    // If it does not, then we are good.
    control.replay();
    webhook.taskChangedState(change);
  }

  @Test
  public void testTaskChangedWithOldState() throws Exception {
    Capture<HttpPost> httpPostCapture = createCapture();
    expect(httpClient.execute(capture(httpPostCapture))).andReturn(null);

    control.replay();

    webhook.taskChangedState(changeWithOldState);

    assertTrue(httpPostCapture.hasCaptured());
    assertEquals(httpPostCapture.getValue().getURI(), new URI("http://localhost:5000/"));
    assertEquals(EntityUtils.toString(httpPostCapture.getValue().getEntity()), changeJson);
    Header[] producerTypeHeader = httpPostCapture.getValue().getHeaders("Producer-Type");
    assertEquals(producerTypeHeader[0].getName(), "Producer-Type");
    assertEquals(producerTypeHeader[0].getValue(), "reliable");
    Header[] contentTypeHeader = httpPostCapture.getValue().getHeaders("Content-Type");
    assertEquals(contentTypeHeader[0].getName(), "Content-Type");
    assertEquals(contentTypeHeader[0].getValue(), "application/vnd.kafka.json.v1+json");
    assertNotNull(httpPostCapture.getValue().getHeaders("Timestamp"));
  }

  @Test
  public void testWebhookInfo() {
    WebhookInfo webhookInfo = WebhookModule.parseWebhookConfig(WebhookModule.readWebhookFile());
    assertEquals(webhookInfo.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURL=http://localhost:5000/, "
            + "connectTimeout=5"
            + "}");
    control.replay();
  }
}
