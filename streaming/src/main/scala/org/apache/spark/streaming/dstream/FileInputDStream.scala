/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import java.io.{FileNotFoundException, IOException, ObjectInputStream}

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming._
import org.apache.spark.util.{TimeStampedHashMap, Utils}

/**
 * This class represents an input stream that monitors a Hadoop-compatible filesystem for new
 * files and creates a stream out of them. The way it works as follows.
 *
 * At each batch interval, the file system is queried for files in the given directory and
 * detected new files are selected for that batch. In this case "new" means files that
 * became visible to readers during that time period. Some extra care is needed to deal
 * with the fact that files may become visible after they are created. For this purpose, this
 * class remembers the information about the files selected in past batches for
 * a certain duration (say, "remember window") as shown in the figure below.
 *
 *                      |<----- remember window ----->|
 * ignore threshold --->|                             |<--- current batch time
 *                      |____.____.____.____.____.____|
 *                      |    |    |    |    |    |    |
 * ---------------------|----|----|----|----|----|----|-----------------------> Time
 *                      |____|____|____|____|____|____|
 *                             remembered batches
 *
 * The trailing end of the window is the "ignore threshold" and all files whose mod times
 * are less than this threshold are assumed to have already been selected and are therefore
 * ignored. Files whose mod times are within the "remember window" are checked against files
 * that have already been selected. At a high level, this is how new files are identified in
 * each batch - files whose mod times are greater than the ignore threshold and
 * have not been considered within the remember window. See the documentation on the method
 * `isNewFile` for more details.
 *
 * This makes some assumptions from the underlying file system that the system is monitoring.
 * - The clock of the file system is assumed to synchronized with the clock of the machine running
 *   the streaming app.
 * - If a file is to be visible in the directory listings, it must be visible within a certain
 *   duration of the mod time of the file. This duration is the "remember window", which is set to
 *   1 minute (see `FileInputDStream.MIN_REMEMBER_DURATION`). Otherwise, the file will never be
 *   selected as the mod time will be less than the ignore threshold when it becomes visible.
 * - Once a file is visible, the mod time cannot change. If it does due to appends, then the
 *   processing semantics are undefined.
 */
private[streaming]
class FileInputDStream[K: ClassTag, V: ClassTag, F <: NewInputFormat[K,V] : ClassTag](
    @transient ssc_ : StreamingContext,
    directory: String,
<<<<<<< HEAD
    depth: Int = 1,
=======
    depth: Int = 0,
>>>>>>> support depth
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true)
  extends InputDStream[(K, V)](ssc_) {

  require(depth >= 1, "nested directories depth must >= 1")
  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = if (newFilesOnly) System.currentTimeMillis() else 0L

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  private val numBatchesToRemember = FileInputDStream.calculateNumBatchesToRemember(slideDuration)
  private val durationToRemember = slideDuration * numBatchesToRemember
  remember(durationToRemember)

  // Map of batch-time to selected file info for the remembered batches
  @transient private[streaming] var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[String]]

  // Set of files that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()
  @transient private val lastFoundDirs = new mutable.HashSet[Path]()


  // Read-through cache of file mod times, used to speed up mod time lookups
  @transient private var fileToModTime = new TimeStampedHashMap[String, Long](true)

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  override def start() { }

  override def stop() { }

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds. And new files may have the same modification time as the
   * latest modification time in the previous call to this method yet was not reported in
   * the previous call.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    // Find new files
    val newFiles = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    batchTimeToSelectedFiles += ((validTime, newFiles))
    recentlySelectedFiles ++= newFiles
    Some(filesToRDD(newFiles))
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
    batchTimeToSelectedFiles --= oldFiles.keys
    recentlySelectedFiles --= oldFiles.values.flatten
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    // Delete file mod times that weren't accessed in the last round of getting new files
    fileToModTime.clearOldValues(lastNewFileFindingTime - 1)
  }

  /**
<<<<<<< HEAD
<<<<<<< HEAD
   * Find new files for the batch of `currentTime`. This is done by first calculating the
   * ignore threshold for file mod times, and then getting a list of files filtered based on
   * the current batch time and the ignore threshold. The ignore threshold is the max of
   * initial ignore threshold and the trailing end of the remember window (that is, which ever
   * is later in time).
=======
   * Find files which have modification timestamp <= current time and  a 3-tuple of
=======
   * Find files which have modification timestamp <= current time and return a 3-tuple of
>>>>>>> support depth
   * (new files found, latest modification time among them, files with latest modification time)
>>>>>>> change Nit
   */

  private def findNewFiles(currentTime: Long): Array[String] = {
    try {
      lastNewFileFindingTime = System.currentTimeMillis

      // Calculate ignore threshold
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold,   // initial threshold based on newFilesOnly setting
        currentTime - durationToRemember.milliseconds  // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")
      val filter = new PathFilter {
        def accept(path: Path): Boolean = isNewFile(path, currentTime, modTimeIgnoreThreshold)
      }
      val directoryDepth = directoryPath.depth()

      //nested directories
      def dfs(status: FileStatus, currentDepth: Int): List[FileStatus] = {
        val modTime = status.getModificationTime
        status match {
          case _ if currentDepth < 0 => Nil
          case _ if !status.isDirectory => {
            if (filter.accept(status.getPath)) {
              status :: Nil
            } else {
              Nil
            }
          }
          case _ if status.isDirectory => {
            val path = status.getPath
            val depthFilter =   depth + directoryDepth - path.depth()
            if (lastFoundDirs.contains(path)
              && (status.getModificationTime > modTimeIgnoreThreshold)) {
              fs.listStatus(path).toList.flatMap(dfs(_, depthFilter - 1))
            } else if (!lastFoundDirs.contains(path) && depthFilter >= 0   ) {
              lastFoundDirs += path
              fs.listStatus(path).toList.flatMap(dfs(_, depthFilter - 1))
            } else {
              Nil
            }
          }
        }
      }

      var newFiles = List[String]()
      if (lastFoundDirs.isEmpty) {
        newFiles = dfs(fs.getFileStatus(directoryPath), depth).map(_.getPath.toString)
      } else {
        lastFoundDirs.filter {
          path =>
            try {
              /* If the modidication time of directory more than ignore time ,the directory
               * is no change.
               */
              val status = fs.getFileStatus(path)
              if (status != null && status.getModificationTime > modTimeIgnoreThreshold) {
                true
              } else {
                false
              }
            }
            catch {
              // If the directory do not found ,remove the drir from lastFoundDirs
              case e: FileNotFoundException => {
                lastFoundDirs.remove(path)
                false
              }
            }
        }.map {
          path =>
            newFiles = fs.listStatus(path).toList.flatMap(dfs(_,
              depth + directoryDepth - path.depth())).map(_.getPath.toString)
        }
      }

      val timeTaken = System.currentTimeMillis - lastNewFileFindingTime
      logInfo("Finding new files took " + timeTaken + " ms")
      logDebug("# cached file times = " + fileToModTime.size)
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          "Time taken to find new files exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directory."
        )
      }
      newFiles.toArray
    } catch {
      case e: Exception =>
        logWarning("Error finding new files", e)
        reset()
        Array.empty
    }
  }

<<<<<<< HEAD
  /**
   * Identify whether the given `path` is a new file for the batch of `currentTime`. For it to be
   * accepted, it has to pass the following criteria.
   * - It must pass the user-provided file filter.
   * - It must be newer than the ignore threshold. It is assumed that files older than the ignore
   *   threshold have already been considered or are existing files before start
   *   (when newFileOnly = true).
   * - It must not be present in the recently selected files that this class remembers.
   * - It must not be newer than the time of the batch (i.e. `currentTime` for which this
   *   file is being tested. This can occur if the driver was recovered, and the missing batches
   *   (during downtime) are being generated. In that case, a batch of time T may be generated
   *   at time T+x. Say x = 5. If that batch T contains file of mod time T+5, then bad things can
   *   happen. Let's say the selected files are remembered for 60 seconds.  At time t+61,
   *   the batch of time t is forgotten, and the ignore threshold is still T+1.
   *   The files with mod time T+5 are not remembered and cannot be ignored (since, t+5 > t+1).
   *   Hence they can get selected as new files again. To prevent this, files whose mod time is more
   *   than current batch time are not considered.
   */
  private def isNewFile(path: Path, currentTime: Long, modTimeIgnoreThreshold: Long): Boolean = {
    val pathStr = path.toString
    if (path.getName().startsWith("_")) {
      logDebug(s"startsWith: ${path.getName()}")
      return false
    }
    // Reject file if it does not satisfy filter
    if (!filter(path)) {
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    // Reject file if it was created before the ignore time
    val modTime = getFileModTime(path)
    if (modTime <= modTimeIgnoreThreshold) {
      // Use <= instead of < to avoid SPARK-4518
      logDebug(s"$pathStr ignored as mod time $modTime <= ignore time $modTimeIgnoreThreshold")
      return false
    }
    // Reject file if mod time > current batch time
    if (modTime > currentTime) {
      logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
      return false
    }
    // Reject file if it was considered earlier
    if (recentlySelectedFiles.contains(pathStr)) {
      logDebug(s"$pathStr already considered")
      return false
    }
    logDebug(s"$pathStr accepted with mod time $modTime")
    return true
=======
  def getPathList(path:Path, fs:FileSystem):List[Path]={
    var pathList = List[Path]()
    pathList = path:: pathList
    var tmp =List[Path]()
    tmp=path::tmp
    for(i <- 0 until depth){
      tmp =getSubPathList(tmp,fs)
      pathList=tmp:::pathList
    }
    pathList
  }

  def getSubPathList(path:List[Path],fs:FileSystem):List[Path]={
    val filter = new SubPathFilter()
    var pathList = List[Path]()
    path.map(subPath=>{
     fs.listStatus(subPath,filter).map(x=>{
        pathList = x.getPath()::pathList
     })
    })
    pathList
>>>>>>> support depth
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    val fileRDDs = files.map(file =>{
      val rdd = context.sparkContext.newAPIHadoopFile[K, V, F](file)
      if (rdd.partitions.size == 0) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    })
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  /** Get file mod time from cache or fetch it from the file system */
  private def getFileModTime(path: Path) = {
    fileToModTime.getOrElseUpdate(path.toString, fs.getFileStatus(path).getModificationTime())
  }

  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    fs_
  }

  private def reset()  {
    fs_ = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(K,V)]] ()
    batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[String]]()
    recentlySelectedFiles = new mutable.HashSet[String]()
    fileToModTime = new TimeStampedHashMap[String, Long](true)
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]

    override def update(time: Time) {
      hadoopFiles.clear()
      hadoopFiles ++= batchTimeToSelectedFiles
    }

    override def cleanup(time: Time) { }

    override def restore() {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          batchTimeToSelectedFiles += ((t, f))
          recentlySelectedFiles ++= f
          generatedRDDs += ((t, filesToRDD(f)))
        }
      }
    }

    override def toString() = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }
}

private[streaming]
object FileInputDStream {

  /**
   * Minimum duration of remembering the information of selected files. Files with mod times
   * older than this "window" of remembering will be ignored. So if new files are visible
   * within this window, then the file will get selected in the next batch.
   */
  private val MIN_REMEMBER_DURATION = Minutes(1)

<<<<<<< HEAD
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  /**
   * Calculate the number of last batches to remember, such that all the files selected in
   * at least last MIN_REMEMBER_DURATION duration can be remembered.
   */
  def calculateNumBatchesToRemember(batchDuration: Duration): Int = {
    math.ceil(MIN_REMEMBER_DURATION.milliseconds.toDouble / batchDuration.milliseconds).toInt
=======
    def accept(path: Path): Boolean = {
      try {
        if (fs.getFileStatus(path).isDirectory()) {
          return false
        }
        if (!filter(path)) {  // Reject file if it does not satisfy filter
          logDebug("Rejected by filter " + path)
          return false
        }
        // Reject file if it was found in the last interval
        if (lastFoundFiles.contains(path.toString)) {
          logDebug("Mod time equal to last mod time, but file considered already")
          return false
        }
        val modTime = getFileModTime(path)
        logDebug(s"Mod time for $path is $modTime")
        if (modTime < ignoreTime) {
          // Reject file if it was created before the ignore time (or, before last interval)
          logDebug(s"Mod time $modTime less than ignore time $ignoreTime")
          return false
        } else if (modTime > maxModTime) {
          // Reject file if it is too new that considering it may give errors
          logDebug("Mod time more than ")
          return false
        }
        if (minNewFileModTime < 0 || modTime < minNewFileModTime) {
          minNewFileModTime = modTime
        }
        if(path.getName().startsWith("_")){
          logDebug(s"startsWith: ${path.getName()}")
          return false
        }
        logDebug("Accepted " + path)
      } catch {
        case fnfe: java.io.FileNotFoundException =>
          logWarning("Error finding new files", fnfe)
          reset()
          false
      }
      true
    }
  }

  class SubPathFilter extends PathFilter {

    def accept(path: Path): Boolean = {
      try {
        val  pathStatus = fs.getFileStatus(path)
        if(pathStatus.isDirectory()){
          val modTime =pathStatus.getModificationTime
          logDebug(s"Mod time for $path is $modTime")
          if (modTime > ignoreTime) {
            logDebug(s"Mod time $modTime more than ignore time $ignoreTime")
            return true
          }
        }
      } catch {
        case fnfe: java.io.IOException =>
          logWarning("Error finding new files", fnfe)
          reset()
          false
      }
      false
    }
>>>>>>> change Nit
  }
}
