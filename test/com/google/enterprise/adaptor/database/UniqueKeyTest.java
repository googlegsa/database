// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.adaptor.database;

import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.adaptor.InvalidConfigurationException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

/** Test cases for {@link UniqueKey}. */
public class UniqueKeyTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  private UniqueKey newUniqueKey(String ukDecls) {
    return newUniqueKey(ukDecls, "", "");
  }

  private UniqueKey newUniqueKey(String ukDecls, String contentSqlColumns,
      String aclSqlColumns) {
    return new UniqueKey(ukDecls, contentSqlColumns, aclSqlColumns, true);
  }

  @Test
  public void testNullKeys() {
    thrown.expect(NullPointerException.class);
    UniqueKey uk = newUniqueKey(null, "", "");
  }

  @Test
  public void testNullContentCols() {
    thrown.expect(NullPointerException.class);
    UniqueKey uk = newUniqueKey("num:int", null, "");
  }

  @Test
  public void testNullAclCols() {
    thrown.expect(NullPointerException.class);
    UniqueKey uk = newUniqueKey("num:int", "", null);
  }

  @Test
  public void testSingleInt() {
    UniqueKey uk = newUniqueKey("numnum:int");
    assertEquals(1, uk.numElementsForTest());
    assertEquals("numnum", uk.nameForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(0));
  }

  @Test
  public void testEmptyThrows() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Invalid db.uniqueKey parameter: value cannot be empty.");
    UniqueKey uk = newUniqueKey(" ");
  }

  @Test
  public void testTwoInt() {
    UniqueKey uk = newUniqueKey("numnum:int,other:int");
    assertEquals(2, uk.numElementsForTest());
    assertEquals("numnum", uk.nameForTest(0));
    assertEquals("other", uk.nameForTest(1));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(1));
  }

  @Test
  public void testIntString() {
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string");
    assertEquals(2, uk.numElementsForTest());
    assertEquals("numnum", uk.nameForTest(0));
    assertEquals("strstr", uk.nameForTest(1));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.STRING, uk.typeForTest(1));
  }

  @Test
  public void testUrlIntColumn() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    new UniqueKey("numnum:int", "", "", false);
  }

  @Test
  public void testUrlTwoColumns() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    UniqueKey uk = new UniqueKey("str1:string,str2:string", "", "", false);
  }

  @Test
  public void testNameRepeatsNotAllowed() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Invalid db.uniqueKey configuration: key name 'num' was repeated.");
    UniqueKey uk = newUniqueKey("num:int,num:string");
  }

  @Test
  public void testCaseSensitiveNameRepeatsAllowed() {
    // If we have multiple column names that differ in both case and type,
    // assume we have a database that supports case-sensitive column names
    // and force the parameter column names to exactly match one of them.
    UniqueKey uk = newUniqueKey("num:int,nuM:int,Num:string");
    assertEquals(3, uk.numElementsForTest());
    assertEquals("num", uk.nameForTest(0));
    assertEquals("nuM", uk.nameForTest(1));
    assertEquals("Num", uk.nameForTest(2));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(1));
    assertEquals(UniqueKey.ColumnType.STRING, uk.typeForTest(2));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest("num"));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest("nuM"));
    assertEquals(UniqueKey.ColumnType.STRING, uk.typeForTest("Num"));
    assertEquals(null, uk.typeForTest("NUM"));
  }

  @Test
  public void testCaseInsensitiveNameNotAllowedIfCaseSensitiveKeys() {
    // If we have multiple column names that differ in both case and type,
    // assume we have a database that supports case-sensitive column names
    // and force the parameter column names to exactly match one of them.
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("unknown column name 'ID'");
    UniqueKey uk = newUniqueKey("id:int,Id:string", "ID", "ID");
  }

  @Test
  public void testCaseInsensitiveNameAllowedIfNotCaseSensitiveKeys() {
    // If there are no case variants in the unique key columns, we can be
    // more lenient in looking up the associated types case-insensitively.
    UniqueKey uk = newUniqueKey("id:int", "Id", "ID");
    assertEquals(1, uk.numElementsForTest());
    assertEquals("id", uk.nameForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest("id"));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest("Id"));
    assertEquals(UniqueKey.ColumnType.INT, uk.typeForTest("ID"));
  }

  @Test
  public void testBadDef() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey definition for 'strstr/string'.");
    UniqueKey uk = newUniqueKey("numnum:int,strstr/string");
  }

  @Test
  public void testUnknownContentCol() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid db.singleDocContentSql value: "
        + "unknown column name 'IsStranger'.");
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string",
        "numnum,IsStranger,strstr", "");
  }

  @Test
  public void testUnknownAclCol() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid db.aclSql value: "
        + "unknown column name 'IsStranger'.");
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string", "",
        "numnum,IsStranger,strstr");
  }

  @Test
  public void testProcessingDocId() throws SQLException {
    executeUpdate("create table data(numnum int, strstr varchar)");
    executeUpdate("insert into data(numnum, strstr) values(345, 'abc')");

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      UniqueKey uk = newUniqueKey("numnum:int,strstr:string");
      assertTrue("ResultSet is empty", rs.next());
      assertEquals("345/abc", uk.makeUniqueId(rs, /*encode=*/ true));
    }
  }

  @Test
  public void testProcessingDocIdWithSlash() throws SQLException {
    executeUpdate("create table data(a varchar, b varchar)");
    executeUpdate("insert into data(a, b) values('5/5', '6/6')");

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      UniqueKey uk = newUniqueKey("a:string,b:string");
      assertTrue("ResultSet is empty", rs.next());
      assertEquals("5_/5/6_/6", uk.makeUniqueId(rs, /*encode=*/ true));
    }
  }

  @Test
  public void testProcessingDocIdWithMoreSlashes() throws SQLException {
    executeUpdate("create table data(a varchar, b varchar)");
    executeUpdate("insert into data(a, b) values('5/5//', '//6/6')");

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      UniqueKey uk = newUniqueKey("a:string,b:string");
      assertTrue("ResultSet is empty", rs.next());
      assertEquals("5_/5_/_/_//_/_/6_/6",
          uk.makeUniqueId(rs, /*encode=*/ true));
    }
  }

  @Test
  public void testPreparingRetrieval() throws SQLException {
    executeUpdate("create table data("
        + "numnum int, strstr varchar, timestamp timestamp, date date, "
        + "time time, long bigint)");

    String sql = "insert into data("
        + "numnum, strstr, timestamp, date, time, long)"
        + " values (?, ?, ?, ?, ?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk = newUniqueKey("numnum:int,strstr:string,"
          + "timestamp:timestamp,date:date,time:time,long:long");
      uk.setContentSqlValues(ps,
          "888/bluesky/1414701070212/2014-01-01/02:03:04/123");
      assertEquals(1, ps.executeUpdate());
    }

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      assertEquals(888, rs.getInt("numnum"));
      assertEquals("bluesky", rs.getString("strstr"));
      assertEquals(new Timestamp(1414701070212L), rs.getTimestamp("timestamp"));
      assertEquals(Date.valueOf("2014-01-01"), rs.getDate("date"));
      assertEquals(Time.valueOf("02:03:04"), rs.getTime("time"));
      assertEquals(123L, rs.getLong("long"));
    }
  }

  @Test
  public void testPreparingRetrievalPerDocCols() throws SQLException {
    executeUpdate("create table data("
        + "col1 int, col2 int, col3 varchar, col4 int, col5 varchar, "
        + "col6 varchar, col7 int)");

    String sql = "insert into data(col1, col2, col3, col4, col5, col6, col7)"
        + " values (?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk = newUniqueKey("numnum:int,strstr:string",
          "numnum,numnum,strstr,numnum,strstr,strstr,numnum", "");
      uk.setContentSqlValues(ps, "888/bluesky");
      assertEquals(1, ps.executeUpdate());
    }

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      assertEquals(888, rs.getInt(1));
      assertEquals(888, rs.getInt(2));
      assertEquals("bluesky", rs.getString(3));
      assertEquals(888, rs.getInt(4));
      assertEquals("bluesky", rs.getString(5));
      assertEquals("bluesky", rs.getString(6));
      assertEquals(888, rs.getInt(7));
    }
  }

  @Test
  public void testPreserveSlashesInColumnValues() throws SQLException {
    executeUpdate("create table data(a varchar, b varchar, c varchar)");

    String sql = "insert into data(a, b, c) values (?, ?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk = newUniqueKey("a:string,b:string", "a,b,a", "");
      uk.setContentSqlValues(ps, "5_/5/6_/6");
      assertEquals(1, ps.executeUpdate());
    }

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      assertEquals("5/5", rs.getString(1));
      assertEquals("6/6", rs.getString(2));
      assertEquals("5/5", rs.getString(3));
    }
  }

  private static String makeId(char choices[], int maxlen) {
    StringBuilder sb = new StringBuilder();
    Random r = new Random();
    int len = r.nextInt(maxlen);
    for (int i = 0; i < len; i++) {
      sb.append(choices[r.nextInt(choices.length)]);
    }
    return "" + sb;
  }

  private static String makeRandomId() {
    char choices[] = "13/\\45\\97_%^&%_$^)*(/<>|P{UI_TY*c".toCharArray();
    return makeId(choices, 100);
  }

  private static String makeSomeIdsWithJustSlashesAndEscapeChar() {
    return makeId("/_".toCharArray(), 100);
  }

  private void testUniqueElementsRoundTrip(String elem1, String elem2)
      throws SQLException {
    try {
      executeUpdate("create table data(a varchar, b varchar)");
      executeUpdate(
          "insert into data(a, b) values('" + elem1 + "', '" + elem2 + "')");

      UniqueKey uk = newUniqueKey("a:string,b:string");
      String id;
      try (Statement stmt = getConnection().createStatement();
          ResultSet rs = stmt.executeQuery("select * from data")) {
        assertTrue("ResultSet is empty", rs.next());
        id = uk.makeUniqueId(rs, /*encode=*/ true);
      }

      String sql = "select * from data where a = ? and b = ?";
      try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
        uk.setContentSqlValues(ps, id);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue("ResultSet is empty", rs.next());
          assertEquals(elem1, rs.getString(1));
          assertEquals(elem2, rs.getString(2));
        }
      } catch (Exception e) {
        throw new RuntimeException("elem1: " + elem1 + ", elem2: " + elem2 
            + ", id: " + id, e);
      }
    } finally {
      dropAllObjects();
    }
  }

  @Test
  public void testFuzzSlashesAndEscapes() throws SQLException {
    for (int fuzzCase = 0; fuzzCase < 1000; fuzzCase++) {
      String elem1 = makeSomeIdsWithJustSlashesAndEscapeChar();
      String elem2 = makeSomeIdsWithJustSlashesAndEscapeChar();
      testUniqueElementsRoundTrip(elem1, elem2);
    }
  }

  @Test
  public void testEmptiesPreserved() throws SQLException {
    testUniqueElementsRoundTrip("", "");
    testUniqueElementsRoundTrip("", "_stuff/");
    testUniqueElementsRoundTrip("_stuff/", "");
  }

  private static String roundtrip(String in) {
    return UniqueKey.decodeSlashInData(UniqueKey.encodeSlashInData(in));
  }

  @Test
  public void testEmpty() {
    assertEquals("", roundtrip(""));
  }

  @Test
  public void testSimpleEscaping() {
    String id = "my-simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithSlash() {
    String id = "my-/simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithEscapeChar() {
    String id = "my-_simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithTripleEscapeChar() {
    String id = "___";
    assertEquals(3, id.length());
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithSlashAndEscapeChar() {
    String id = "my-_simp/le-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithMessOfSlashsAndEscapeChars() {
    String id = "/_/_my-_/simp/le-id/_/______//_";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testFuzzEncodeAndDecode() {
    int ntests = 100;
    for (int i = 0; i < ntests; i++) {
      String fuzz = makeRandomId();
      assertEquals(fuzz, roundtrip(fuzz));
    }
  }

  @Test
  public void testSpacesBetweenUniqueKeyDeclerations() {
    UniqueKey uk = newUniqueKey("numnum:int,other:int");
    assertEquals(uk, newUniqueKey(" numnum:int,other:int"));
    assertEquals(uk, newUniqueKey("numnum:int ,other:int"));
    assertEquals(uk, newUniqueKey("numnum:int, other:int"));
    assertEquals(uk, newUniqueKey("numnum:int , other:int"));
    assertEquals(uk, newUniqueKey("   numnum:int  ,  other:int   "));
  }

  @Test
  public void testSpacesWithinUniqueKeyDeclerations() {
    UniqueKey uk = newUniqueKey("numnum:int,other:int");
    assertEquals(uk, newUniqueKey("numnum :int,other:int"));
    assertEquals(uk, newUniqueKey("numnum: int,other:int"));
    assertEquals(uk, newUniqueKey("numnum : int,other:int"));
    assertEquals(uk, newUniqueKey("numnum : int,other   :    int"));
  }

  @Test
  public void testSpacesBetweenContentSqlCols() {
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string",
        "numnum,numnum,strstr,numnum,strstr,strstr,numnum", "");
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "numnum ,numnum,strstr,numnum,strstr,strstr,numnum", ""));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "numnum, numnum,strstr,numnum,strstr,strstr,numnum", ""));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "numnum , numnum,strstr,numnum,strstr,strstr,numnum", ""));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "numnum  ,   numnum,strstr,numnum,strstr,strstr,numnum", ""));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "numnum  ,   numnum , strstr   ,  numnum,strstr,strstr, numnum ", ""));
  }

  @Test
  public void testSpacesBetweenAclSqlCols() {
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string",
        "", "numnum,numnum,strstr,numnum,strstr,strstr,numnum");
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "", "numnum ,numnum,strstr,numnum,strstr,strstr,numnum"));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "", "numnum, numnum,strstr,numnum,strstr,strstr,numnum"));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "", "numnum , numnum,strstr,numnum,strstr,strstr,numnum"));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "", "numnum  ,   numnum,strstr,numnum,strstr,strstr,numnum"));
    assertEquals(uk, newUniqueKey("numnum:int,strstr:string",
        "", "numnum  ,   numnum , strstr   ,  numnum,strstr,strstr, numnum "));
  }
}
