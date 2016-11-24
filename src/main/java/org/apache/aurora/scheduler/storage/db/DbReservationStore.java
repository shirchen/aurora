package org.apache.aurora.scheduler.storage.db;


import com.google.inject.Inject;
import org.apache.aurora.scheduler.storage.ReservationStore;
import org.apache.ibatis.annotations.Insert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

class DbReservationStore implements ReservationStore.Mutable {

  private static final Logger LOG = LoggerFactory.getLogger(ReservationStore.Mutable.class);

  private final ReservedTasksMapper mapper;

  @Inject
  DbReservationStore(ReservedTasksMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper);
  }

  public void removeTaskId(String taskId) {
    LOG.info("Deleting " + taskId);
    mapper.delete(Objects.requireNonNull(taskId));
  }

  public void saveReserervedTasks(String taskId) {
    mapper.insert(Objects.requireNonNull(taskId));
  }

  public Set<String> fetchReservedTasks() {
    return mapper.selectAll();
  }

  public void deleteReservedTasks() {
    mapper.truncate();
  }
}
