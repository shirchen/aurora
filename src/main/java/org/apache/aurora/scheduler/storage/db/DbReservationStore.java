package org.apache.aurora.scheduler.storage.db;


import org.apache.aurora.scheduler.storage.ReservationStore;

import java.util.HashSet;

class DbReservationStore implements ReservationStore.Mutable {

  public void removeTaskId(String taskId) {

  }

  public void saveReserervedTasks(String taskId) {

  }

  public HashSet<String> fetchReservedTasks() {
    HashSet<String> reservedTasks = new HashSet<>();
    return reservedTasks;
  }

}
