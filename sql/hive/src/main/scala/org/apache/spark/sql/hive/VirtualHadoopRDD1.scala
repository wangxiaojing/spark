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

package org.apache.spark.sql.hive

import java.text.SimpleDateFormat
import java.util.Date
import java.io.EOFException

import org.apache.spark.rdd.{HadoopPartition, HadoopRDD}

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.util.{NextIterator, Utils}
import org.apache.spark.scheduler.{HostTaskLocation, HDFSCacheTaskLocation}
import org.apache.hadoop.hive.ql.plan.TableDesc

/**
 * Created by ocquery on 3/13/15.
 */
class VirtualHadoopRDD[K, V](
     sc: SparkContext,
     broadcastedConf: Broadcast[SerializableWritable[Configuration]],
     initLocalJobConfFuncOpt: Option[JobConf => Unit],
     inputFormatClass: Class[_ <: InputFormat[K, V]],
     keyClass: Class[K],
     valueClass: Class[V],
     minPartitions: Int,
    tableDesc:TableDesc)
  extends HadoopRDD(sc,
    broadcastedConf,
    initLocalJobConfFuncOpt,
    inputFormatClass,
    keyClass,valueClass,minPartitions) with Logging {

   val fieldDelim = tableDesc.getProperties().getProperty("field.delim","\001")
   val lineDelim = tableDesc.getProperties().getProperty("line.delim")

  private val createTime = new Date()
  

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      val jobConf = getJobConf()

      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = if (split.inputSplit.value.isInstanceOf[FileSplit]) {
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback(
          split.inputSplit.value.asInstanceOf[FileSplit].getPath, jobConf)
      } else {
        None
      }
      if (bytesReadCallback.isDefined) {
        context.taskMetrics.inputMetrics = Some(inputMetrics)
      }

      var reader: RecordReader[K, V] = null
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(createTime),
        context.stageId, theSplit.index, context.attemptId.toInt, jobConf)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }
      val key: K = reader.createKey()
      val value: V = reader.createValue()
      val pathValue = split.inputSplit.value.toString.split(":")
      val path = pathValue(0) + ":" + pathValue(1)

      var recordsSinceMetricsUpdate = 0

      override def getNext() = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }

        // Update bytes read metric every few records
        if (recordsSinceMetricsUpdate == HadoopRDD.RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES
          && bytesReadCallback.isDefined) {
          recordsSinceMetricsUpdate = 0
          val bytesReadFn = bytesReadCallback.get
          inputMetrics.bytesRead = bytesReadFn()
        } else {
          recordsSinceMetricsUpdate += 1
        }
        val t = value.toString  + fieldDelim + key.toString + fieldDelim + path
        val t2 = TextValue.set(t)
        (key, TextValue.get.asInstanceOf[V])
      }

      override def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            val bytesReadFn = bytesReadCallback.get
            inputMetrics.bytesRead = bytesReadFn()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.bytesRead = split.inputSplit.value.getLength
              context.taskMetrics.inputMetrics = Some(inputMetrics)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        } catch {
          case e: Exception => {
            if (!Utils.inShutdown()) {
              logWarning("Exception in RecordReader.close()", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

}

object TextValue{
  val word = new Text()

   def set(value:String){
    word.set(value)
  }

  def get(): Text= {
    word
  }
}

