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

package org.apache.spark.sql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import javax.print.attribute.standard.Compression

import org.apache.spark.io.CompressionCodec

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.{MemoryBlock, TaskMemoryManager}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A collection of implicit methods for converting common Scala objects into [[DataFrame]]s.
 */
private[sql] abstract class SQLImplicits {
  protected def _sqlContext: SQLContext

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 1.3.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * @since 1.3.0
   */
  implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder = {
    DataFrameHolder(_sqlContext.createDataFrame(rdd))
  }

  /**
   * Creates a DataFrame from a local Seq of Product.
   * @since 1.3.0
   */
  implicit def localSeqToDataFrameHolder[A <: Product : TypeTag](data: Seq[A]): DataFrameHolder =
  {
    DataFrameHolder(_sqlContext.createDataFrame(data))
  }

  // Do NOT add more implicit conversions. They are likely to break source compatibility by
  // making existing implicit conversions ambiguous. In particular, RDD[Double] is dangerous
  // because of [[DoubleRDDFunctions]].

  /**
   * Creates a single column DataFrame from an RDD[Int].
   * @since 1.3.0
   */
  implicit def intRddToDataFrameHolder(data: RDD[Int]): DataFrameHolder = {
    val dataType = IntegerType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setInt(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[Long].
   * @since 1.3.0
   */
  implicit def longRddToDataFrameHolder(data: RDD[Long]): DataFrameHolder = {
    val dataType = LongType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setLong(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[String].
   * @since 1.3.0
   */
  implicit def stringRddToDataFrameHolder(data: RDD[String]): DataFrameHolder = {
    val dataType = StringType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.update(0, UTF8String.fromString(v))
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * ::Experimental::
   *
   * Pimp my library decorator for tungsten caching of DataFrames.
   * @since 1.5.1
   */
  @Experimental
  implicit class TungstenCache(df: DataFrame) {
    /**
     * Packs the rows of [[df]] into contiguous blocks of memory.
     * @param compressionType "" (default), "lz4", "lzf", or "snappy", see
     *                        [[CompressionCodec.ALL_COMPRESSION_CODECS]]
     */
    def tungstenCache(compressionType: String = ""): (RDD[_], DataFrame) = {
      val BLOCK_SIZE = 4000000 // 4 MB blocks
      val schema = df.schema

      val convert = CatalystTypeConverters.createToCatalystConverter(schema)
      val internalRows = df.rdd.map(convert(_).asInstanceOf[InternalRow])
      val cachedRDD = internalRows.mapPartitions { rowIterator =>
        val bufferedRowIterator = rowIterator.buffered

        val convertToUnsafe = UnsafeProjection.create(schema)
        val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.executorMemoryManager)
        val compressionCodec: Option[CompressionCodec] = if (compressionType.isEmpty)
          None
        else
          Some(CompressionCodec.createCodec(SparkEnv.get.conf, compressionType))
        new Iterator[MemoryBlock] {

          // This assumes that size of row < BLOCK_SIZE
          def next(): MemoryBlock = {
            val block = taskMemoryManager.allocateUnchecked(BLOCK_SIZE)
            var currOffset = 0

            while (bufferedRowIterator.hasNext && currOffset < BLOCK_SIZE) {
              val currRow = convertToUnsafe.apply(bufferedRowIterator.head)
              val recordSize = 4 + currRow.getSizeInBytes

              if (currOffset + recordSize < BLOCK_SIZE) {
                // Pack into memory with layout [rowSize (4) | row (rowSize)]
                Platform.putInt(
                  block.getBaseObject, block.getBaseOffset + currOffset, currRow.getSizeInBytes)
                currRow.writeToMemory(
                  block.getBaseObject, block.getBaseOffset + currOffset + 4)
                bufferedRowIterator.next()
              }
              currOffset += recordSize // Increment regardless to break loop when full
            }

            // Optionally compress block before writing
            compressionCodec match {
              case Some(codec) =>
                val blockArray = new Array[Byte](BLOCK_SIZE)
                Platform.copyMemory(
                  block.getBaseObject,
                  block.getBaseOffset,
                  blockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  block.size())
                val compressedBaos = new ByteArrayOutputStream(BLOCK_SIZE)
                codec.compressedOutputStream(compressedBaos).write(blockArray)
                val compressedBlockArray = compressedBaos.toByteArray

                // Pad block to word boundary
                val padding = ((8 - (compressedBlockArray.size % 8)) % 8) + 4
                val compressedBlock = taskMemoryManager.allocateUnchecked(
                  4 + compressedBlockArray.size + padding)

                // Store [padding(4)|compressedBlock]
                Platform.putInt(
                  compressedBlock.getBaseObject,
                  compressedBlock.getBaseOffset,
                  padding)
                Platform.copyMemory(
                  compressedBlockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  compressedBlock.getBaseObject,
                  compressedBlock.getBaseOffset + 4,
                  compressedBlockArray.size)
                taskMemoryManager.freeUnchecked(block)
                compressedBlock
              case None => block
            }
          }

          def hasNext: Boolean = bufferedRowIterator.hasNext
        }
      }.persist(StorageLevel.MEMORY_ONLY)

      val baseRelation: BaseRelation = new BaseRelation with TableScan {
        override val sqlContext = _sqlContext
        override val schema = df.schema
        override val needConversion = false

        override def buildScan(): RDD[Row] = {
          val numFields = this.schema.length
          val _compressionType: String = compressionType

          cachedRDD.flatMap { rawBlock =>
            val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.executorMemoryManager)
            val compressionCodec: Option[CompressionCodec] = if (_compressionType.isEmpty)
              None
            else
              Some(CompressionCodec.createCodec(SparkEnv.get.conf, _compressionType))
            val block = compressionCodec match {
              case Some(codec) =>
                val compressedBlockArray = new Array[Byte](rawBlock.size().toInt)
                val padding = Platform.getInt(rawBlock.getBaseObject, rawBlock.getBaseOffset)
                Platform.copyMemory(
                  rawBlock.getBaseObject,
                  rawBlock.getBaseOffset + 4,
                  compressedBlockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  rawBlock.size() - padding)
                val uncompressedBaos = new ByteArrayInputStream(compressedBlockArray)
                val uncompressedBlockArray = new Array[Byte](uncompressedBaos.available())
                codec.compressedInputStream(uncompressedBaos).read(uncompressedBlockArray)
                val uncompressedBlock = taskMemoryManager.allocateUnchecked(
                  uncompressedBlockArray.size)
                Platform.copyMemory(
                  uncompressedBlockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  uncompressedBlock.getBaseObject,
                  uncompressedBlock.getBaseOffset,
                  uncompressedBlockArray.size)
                taskMemoryManager.freeUnchecked(rawBlock)
                uncompressedBlock
              case None => rawBlock
            }
            val rows = new ArrayBuffer[InternalRow]()
            var currOffset = 0
            var moreData = true
            while (currOffset < block.size() && moreData) {
              val rowSize = Platform.getInt(block.getBaseObject, block.getBaseOffset + currOffset)
              currOffset += 4
              if (rowSize > 0) {
                val unsafeRow = new UnsafeRow()
                unsafeRow.pointTo(
                  block.getBaseObject, block.getBaseOffset + currOffset, numFields, rowSize)
                rows.append(unsafeRow)
                currOffset += rowSize
              } else {
                moreData = false
              }
            }
            rows
          }.asInstanceOf[RDD[Row]]
        }

        override def toString: String = getClass.getSimpleName + s"[${df.toString}]"
      }
      (cachedRDD, DataFrame(_sqlContext, LogicalRelation(baseRelation)))
    }
  }
}
