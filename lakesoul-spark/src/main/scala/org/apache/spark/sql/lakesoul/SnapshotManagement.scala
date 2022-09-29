/*
 * Copyright [2022] [DMetaSoul Team]
 *
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

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{MetaUtils, MetaVersion}
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import javolution.util.ReentrantLock
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.{PartitionInfo, SparkUtil, TableInfo}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

class SnapshotManagement(path: String) extends Logging {

  val table_path: String = path

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: Snapshot = getCurrentSnapshot

  def snapshot: Snapshot = currentSnapshot

  private def createSnapshot: Snapshot = {
    val table_info = MetaVersion.getTableInfo(table_path)
    val partition_info_arr = MetaVersion.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw LakeSoulErrors.schemaNotSetException
    }
    new Snapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: Snapshot = {
    val table_id = "table_" + UUID.randomUUID().toString
    val table_info = TableInfo(LakeSoulCatalog.currentDefaultNamespace.head, Some(table_path), table_id)
    val partition_arr = Array(
      PartitionInfo(table_id, MetaUtils.DEFAULT_RANGE_PARTITION_VALUE,0)
    )
    new Snapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: Snapshot = {

    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be lakesoul table
      if (LakeSoulUtils.isLakeSoulTable(table_path)) {
        throw new AnalysisException("table_name is expected as root path in SnapshotManagement")
      }
      initSnapshot
    }
  }

  def updateSnapshot(): Snapshot = {
    lockInterruptibly {
      val new_snapshot = getCurrentSnapshot
      currentSnapshot = new_snapshot
      currentSnapshot
    }
  }

  def updateSnapshotForVersion(partitionDesc: String, partitionVersion: Int): Unit = {
    lockInterruptibly {
      currentSnapshot.setPartitionDescAndVersion(partitionDesc, partitionVersion)
    }
  }

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      MetaVersion.getTableInfo(table_path)
    } else {
      val table_id = "table_" + UUID.randomUUID().toString
      // TODO: unwrap namespaces: array[string] here
      TableInfo(LakeSoulCatalog.currentDefaultNamespace.head, Some(table_path), table_id)
    }
  }

  def startTransaction(): TransactionCommit = {
    updateSnapshot()
    new TransactionCommit(this)
  }

  /**
    * Execute a piece of code within a new [[TransactionCommit]]. Reads/write sets will
    * be recorded for this table, and all other tables will be read
    * at a snapshot that is pinned on the first access.
    *
    * @note This uses thread-local variable to make the active transaction visible. So do not use
    *       multi-threaded code in the provided thunk.
    */
  def withNewTransaction[T](thunk: TransactionCommit => T): T = {
    try {
      val tc = startTransaction()
      TransactionCommit.setActive(tc)
      thunk(tc)
    } finally {
      TransactionCommit.clearActive()
    }
  }

  /**
    * Checks whether this table only accepts appends. If so it will throw an error in operations that
    * can remove data such as DELETE/UPDATE/MERGE.
    */
  def assertRemovable(): Unit = {
    if (LakeSoulConfig.IS_APPEND_ONLY.fromTableInfo(snapshot.getTableInfo)) {
      throw LakeSoulErrors.modifyAppendOnlyTableException
    }
  }

  def lockInterruptibly[T](body: => T): T = {
    lock.lock()
    try {
      body
    } finally {
      lock.unlock()
    }
  }

}

object SnapshotManagement {

  /**
    * We create only a single [[SnapshotManagement]] for any given path to avoid wasted work
    * in reconstructing.
    */
  private val snapshotManagementCache = {
    val expireMin = if (SparkSession.getActiveSession.isDefined) {
      SparkSession.getActiveSession.get.conf.get(LakeSoulSQLConf.SNAPSHOT_CACHE_EXPIRE)
    } else {
      LakeSoulSQLConf.SNAPSHOT_CACHE_EXPIRE.defaultValue.get
    }
    val builder = CacheBuilder.newBuilder()
      .expireAfterWrite(expireMin, TimeUnit.MINUTES)
      .removalListener((removalNotification: RemovalNotification[String, SnapshotManagement]) => {
        val snapshotManagement = removalNotification.getValue
        try snapshotManagement.snapshot catch {
          case _: NullPointerException =>
          // Various layers will throw null pointer if the RDD is already gone.
        }
      })

    builder.maximumSize(64).build[String, SnapshotManagement]()
  }

  def forTable(spark: SparkSession, tableName: TableIdentifier): SnapshotManagement = {
    val path = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(tableName)
    apply(new Path(path.getOrElse(SparkUtil.getDefaultTablePath(tableName).toString)))
  }

  def forTable(dataPath: File): SnapshotManagement = {
    apply(new Path(dataPath.getAbsolutePath))
  }

  def apply(path: Path): SnapshotManagement = apply(path.toString)

  def apply(path: String): SnapshotManagement = {
    try {
      val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      snapshotManagementCache.get(qualifiedPath, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new SnapshotManagement(qualifiedPath)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }

  //no cache just for snapshot
  def apply(path: String, partitionDesc: String, partitionVersion: Int): SnapshotManagement = {
    val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    if (LakeSoulSourceUtils.isLakeSoulTableExists(qualifiedPath)) {
      val sm = apply(qualifiedPath)
      sm.updateSnapshotForVersion(partitionDesc, partitionVersion)
      apply(qualifiedPath)
    } else {
      throw new AnalysisException("table not exitst in the path;")
    }
  }

  def invalidateCache(path: String): Unit = {
    val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    snapshotManagementCache.invalidate(qualifiedPath)
  }

  def clearCache(): Unit = {
    snapshotManagementCache.invalidateAll()
  }
}
