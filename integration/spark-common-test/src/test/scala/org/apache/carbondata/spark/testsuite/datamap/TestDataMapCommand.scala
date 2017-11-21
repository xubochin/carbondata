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

package org.apache.carbondata.spark.testsuite.datamap

import java.util

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta}
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter
import org.apache.carbondata.core.datamap.dev.cgdatamap.{AbstractCoarseGrainDataMap, AbstractCoarseGrainDataMapFactory}
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.events.Event

class TestDataMapCommand extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
    sql("create table datamaptest (a string, b string, c string) stored by 'carbondata'")
  }
  
  test("test datamap create") {
    sql(s"create datamap datamap1 on table datamaptest using '${new TestDataMapFactory().getClass.getName}'")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 1)
    assert(dataMapSchemaList.get(0).getDataMapName.equals("datamap1"))
    assert(dataMapSchemaList.get(0).getClassName.equals(s"${new TestDataMapFactory().getClass.getName}"))
  }

  test("test datamap create with dmproperties") {
    sql(s"create datamap datamap2 on table datamaptest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
    assert(dataMapSchemaList.get(1).getDataMapName.equals("datamap2"))
    assert(dataMapSchemaList.get(1).getClassName.equals(s"${new TestDataMapFactory().getClass.getName}"))
    assert(dataMapSchemaList.get(1).getProperties.get("key").equals("value"))
  }

  test("test datamap create with existing sname") {
    intercept[Exception] {
      sql(
        s"create datamap datamap2 on table datamaptest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
  }

  test("test datamap create with preagg") {
    sql("drop datamap if exists datamap3 on table datamaptest")
    sql(
      "create datamap datamap3 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
    assert(dataMapSchemaList.get(2).getDataMapName.equals("datamap3"))
    assert(dataMapSchemaList.get(2).getProperties.get("key").equals("value"))
    assert(dataMapSchemaList.get(2).getChildSchema.getTableName.equals("datamaptest_datamap3"))
  }

  test("test datamap create with preagg with duplicate name") {
    intercept[Exception] {
      sql(
        "create datamap datamap2 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }

  test("test datamap drop with preagg") {
    intercept[Exception] {
      sql("drop table datamap3")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }

  test("test show datamap without preaggregate") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql(s"create datamap datamap1 on table datamapshowtest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    sql(s"create datamap datamap2 on table datamapshowtest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    checkExistence(sql("show datamap on table datamapshowtest"), true, "datamap1", "datamap2", "(NA)", s"${new TestDataMapFactory().getClass.getName}")
  }

  test("test show datamap with preaggregate") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
    sql(s"create datamap datamap2 on table datamapshowtest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    val frame = sql("show datamap on table datamapshowtest")
    assert(frame.collect().length == 2)
    checkExistence(frame, true, "datamap1", "datamap2", "(NA)", s"${new TestDataMapFactory().getClass.getName}", "default.datamapshowtest_datamap1")
  }

  test("test show datamap with no datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    assert(sql("show datamap on table datamapshowtest").collect().length == 0)
  }

  test("test show datamap after dropping datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
    sql(s"create datamap datamap2 on table datamapshowtest using '${new TestDataMapFactory().getClass.getName}' dmproperties('key'='value')")
    sql("drop datamap datamap1 on table datamapshowtest")
    val frame = sql("show datamap on table datamapshowtest")
    assert(frame.collect().length == 1)
    checkExistence(frame, true, "datamap2", "(NA)", s"${new TestDataMapFactory().getClass.getName}")
  }


  override def afterAll {
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
  }

}

class TestDataMapFactory extends AbstractCoarseGrainDataMapFactory {
  /**
   * Initialization of Datamap factory with the identifier and datamap name
   */
  override def init(identifier: AbsoluteTableIdentifier,
      dataMapSchema: DataMapSchema): Unit = {

  }

  /**
   * Return a new write for this datamap
   */
  override def createWriter(segmentId: String,
      writeDirectoryPath: String): AbstractDataMapWriter = ???

  /**
   * Get the datamap for segmentid
   */
  override def getDataMaps(segmentId: String): util.List[AbstractCoarseGrainDataMap] = ???

  /**
   * Get datamaps for distributable object.
   */
  override def getDataMaps(distributable: DataMapDistributable): util
  .List[AbstractCoarseGrainDataMap] = ???

  /**
   * Get all distributable objects of a segmentid
   *
   * @return
   */
  override def toDistributable(segmentId: String): util.List[DataMapDistributable] = ???

  /**
   *
   * @param event
   */
  override def fireEvent(event: Event): Unit = ???

  /**
   * Clears datamap of the segment
   */
  override def clear(segmentId: String): Unit = ???

  /**
   * Clear all datamaps from memory
   */
  override def clear(): Unit = {}

  /**
   * Return metadata of this datamap
   */
  override def getMeta: DataMapMeta = {
    ???
  }
}
