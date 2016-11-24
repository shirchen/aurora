package org.apache.aurora.scheduler.storage.db;

import org.apache.ibatis.annotations.Param;

import java.util.Set;

/**
 * MyBatis mapper class for ReservedTasksMapper.xml.
 */
interface ReservedTasksMapper {

  /* Insert taskId into list of tasks needing a dynamic reservation */
  void insert(@Param("taskId") String taskId);

  /* Gets all taskIds that need a dynamic reservation */
  Set<String> selectAll();

  /* Delete taskId from list of reserved tasks
  */
  void delete(@Param("taskId") String taskId);

  /**
   * Removes all stored reserved tasks.
   */
  void truncate();
}
