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
package org.apache.aurora.common.application.modules;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.NumericStatExporter;

/**
 * Module to enable periodic exporting of registered stats to an external service.
 *
 * This modules supports a single command line argument, {@code stat_export_interval}, which
 * controls the export interval (defaulting to 1 minute).
 *
 * Bindings required by this module:
 * <ul>
 *   <li>{@code @ShutdownStage ShutdownRegistry} - Shutdown action registry.
 * </ul>
 *
 * @author William Farner
 */
public class StatsExportModule extends AbstractModule {

  @CmdLine(name = "stat_export_interval",
           help = "Amount of time to wait between stat exports.")
  private static final Arg<Amount<Long, Time>> EXPORT_INTERVAL =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @Override
  protected void configure() {
    requireBinding(Key.get(new TypeLiteral<Closure<Map<String, ? extends Number>>>() { }));
    LifecycleModule.bindStartupAction(binder(), StartCuckooExporter.class);
  }

  public static final class StartCuckooExporter implements Command {

    private final Closure<Map<String, ? extends Number>> statSink;
    private final ShutdownRegistry shutdownRegistry;

    @Inject StartCuckooExporter(
        Closure<Map<String, ? extends Number>> statSink,
        ShutdownRegistry shutdownRegistry) {

      this.statSink = Preconditions.checkNotNull(statSink);
      this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
    }

    @Override public void execute() {
      ThreadFactory threadFactory =
          new ThreadFactoryBuilder().setNameFormat("CuckooExporter-%d").setDaemon(true).build();

      final NumericStatExporter exporter = new NumericStatExporter(statSink,
          Executors.newScheduledThreadPool(1, threadFactory), EXPORT_INTERVAL.get());

      exporter.start(shutdownRegistry);
    }
  }
}