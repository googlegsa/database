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

import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static com.google.enterprise.adaptor.database.UniqueKey.ColumnType;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.google.enterprise.adaptor.InvalidConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/** Test cases for {@link UniqueKey}. */
public class UniqueKeyTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  private UniqueKey newUniqueKey(String ukDecls) {
    return new UniqueKey.Builder(ukDecls).build();
  }

  private UniqueKey newUniqueKey(String ukDecls, String contentSqlColumns,
      String aclSqlColumns) {
    return new UniqueKey.Builder(ukDecls)
        .setContentSqlColumns(contentSqlColumns)
        .setAclSqlColumns(aclSqlColumns)
        .build();
  }

  @Test
  public void testNullKeys() {
    thrown.expect(NullPointerException.class);
    new UniqueKey.Builder(null);
  }

  @Test
  public void testNullContentCols() {
    thrown.expect(NullPointerException.class);
    new UniqueKey.Builder("num:int").setContentSqlColumns(null);
  }

  @Test
  public void testNullAclCols() {
    thrown.expect(NullPointerException.class);
    new UniqueKey.Builder("num:int").setAclSqlColumns(null);
  }

  @Test
  public void testSingleInt() {
    UniqueKey.Builder builder = new UniqueKey.Builder("numnum:int");
    assertEquals(asList("numnum"), builder.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, builder.getColumnTypes().get("numnum"));
  }

  @Test
  public void testEmptyThrows() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Invalid db.uniqueKey parameter: value cannot be empty.");
    new UniqueKey.Builder(" ");
  }

  @Test
  public void testTwoInt() {
    UniqueKey.Builder builder = new UniqueKey.Builder("numnum:int,other:int");
    assertEquals(asList("numnum", "other"), builder.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, builder.getColumnTypes().get("numnum"));
    assertEquals(ColumnType.INT, builder.getColumnTypes().get("other"));
  }

  @Test
  public void testIntString() {
    UniqueKey.Builder builder
        = new UniqueKey.Builder("numnum:int,strstr:string");
    assertEquals(asList("numnum", "strstr"), builder.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, builder.getColumnTypes().get("numnum"));
    assertEquals(ColumnType.STRING, builder.getColumnTypes().get("strstr"));
  }

  @Test
  public void testUrlIntColumn() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    new UniqueKey.Builder("numnum:int").setDocIdIsUrl(true).build();
  }

  @Test
  public void testUrlTwoColumns() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    new UniqueKey.Builder("str1:string,str2:string")
        .setDocIdIsUrl(true).build();
  }

  @Test
  public void testUrlColumnMissingType() {
    thrown.expect(InvalidConfigurationException.class);
    new UniqueKey.Builder("url").setDocIdIsUrl(true).build();
  }

  @Test
  public void testNameRepeatsNotAllowed() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Invalid db.uniqueKey configuration: key name 'num' was repeated.");
    new UniqueKey.Builder("NUM:int,num:string");
  }

  @Test
  public void testBadDef() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey type 'invalid' for 'strstr'.");
    new UniqueKey.Builder("numnum:int,strstr:invalid");
  }

  @Test
  public void testUnknownType() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Unknown column type for the following columns: [id]");
    new UniqueKey.Builder("id").build();
  }

  @Test
  public void testUnknownContentCol() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Unknown column 'IsStranger' from db.singleDocContentSqlParameters");
    new UniqueKey.Builder("numnum:int,strstr:string")
        .setContentSqlColumns("numnum,IsStranger,strstr");
  }

  @Test
  public void testUnknownAclCol() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Unknown column 'IsStranger' from db.aclSqlParameters");
    new UniqueKey.Builder("numnum:int,strstr:string")
        .setAclSqlColumns("numnum,IsStranger,strstr");
  }

  @Test
  public void testAddColumnTypes() {
    Map<String, Integer> sqlTypes = new HashMap<>();
    sqlTypes.put("BIT", Types.BIT);
    sqlTypes.put("BOOLEAN", Types.BOOLEAN);
    sqlTypes.put("TINYINT", Types.TINYINT);
    sqlTypes.put("SMALLINT", Types.SMALLINT);
    sqlTypes.put("INTEGER", Types.INTEGER);
    sqlTypes.put("BIGINT", Types.BIGINT);
    sqlTypes.put("CHAR", Types.CHAR);
    sqlTypes.put("VARCHAR", Types.VARCHAR);
    sqlTypes.put("LONGVARCHAR", Types.LONGVARCHAR);
    sqlTypes.put("NCHAR", Types.NCHAR);
    sqlTypes.put("NVARCHAR", Types.NVARCHAR);
    sqlTypes.put("LONGNVARCHAR", Types.LONGNVARCHAR);
    sqlTypes.put("DATALINK", Types.DATALINK);
    sqlTypes.put("DATE", Types.DATE);
    sqlTypes.put("TIME", Types.TIME);
    sqlTypes.put("TIMESTAMP", Types.TIMESTAMP);

    Map<String, ColumnType> golden
        = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    golden.put("BIT", ColumnType.INT);
    golden.put("BOOLEAN", ColumnType.INT);
    golden.put("TINYINT", ColumnType.INT);
    golden.put("SMALLINT", ColumnType.INT);
    golden.put("INTEGER", ColumnType.INT);
    golden.put("BIGINT", ColumnType.LONG);
    golden.put("CHAR", ColumnType.STRING);
    golden.put("VARCHAR", ColumnType.STRING);
    golden.put("LONGVARCHAR", ColumnType.STRING);
    golden.put("NCHAR", ColumnType.STRING);
    golden.put("NVARCHAR", ColumnType.STRING);
    golden.put("LONGNVARCHAR", ColumnType.STRING);
    golden.put("DATALINK", ColumnType.STRING);
    golden.put("DATE", ColumnType.DATE);
    golden.put("TIME", ColumnType.TIME);
    golden.put("TIMESTAMP", ColumnType.TIMESTAMP);

    UniqueKey.Builder builder = new UniqueKey.Builder("BIT, BOOLEAN, TINYINT, "
        + "SMALLINT, INTEGER, BIGINT, CHAR, VARCHAR, LONGVARCHAR, NCHAR, "
        + "NVARCHAR, LONGNVARCHAR, DATALINK, DATE, TIME, TIMESTAMP");
    builder.addColumnTypes(sqlTypes);
    assertEquals(golden, builder.getColumnTypes());
  }

  @Test
  public void testAddColumnTypes_invalidType() {
    Map<String, Integer> sqlTypes = new HashMap<>();
    sqlTypes.put("blob", Types.BLOB);

    UniqueKey.Builder builder = new UniqueKey.Builder("blob");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey SQLtype");
    builder.addColumnTypes(sqlTypes);
  }

  @Test
  public void testProcessingDocId() throws Exception {
    executeUpdate("create table data(numnum int, strstr varchar)");
    executeUpdate("insert into data(numnum, strstr) values(345, 'abc')");

    ResultSet rs = executeQueryAndNext("select * from data");
    UniqueKey uk = newUniqueKey("numnum:int,strstr:string");
    assertEquals("345/abc", uk.makeUniqueId(rs));
  }

  @Test
  public void testProcessingDocId_allTypes() throws Exception {
    executeUpdate("create table data(c1 integer, c2 bigint, c3 varchar, "
        + "c4 date, c5 time, c6 timestamp)");
    executeUpdate("insert into data(c1, c2, c3, c4, c5, c6) "
        + "values (123, 4567890, 'foo', "
        + "{d '2007-08-09'}, {t '12:34:56'}, {ts '2007-08-09 12:34:56'})");

    UniqueKey uk = new UniqueKey.Builder(
        "c1:int, c2:long, c3:string, c4:date, c5:time, c6:timestamp").build();
    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals("123/4567890/foo/2007-08-09/12:34:56/"
                 + Timestamp.valueOf("2007-08-09 12:34:56").getTime(),
                 uk.makeUniqueId(rs));
  }

  @Test
  public void testProcessingDocId_docIdIsUrl() throws Exception {
    executeUpdate("create table data(url varchar)");
    executeUpdate("insert into data(url) values('http://localhost/foo/bar')");

    ResultSet rs = executeQueryAndNext("select * from data");
    UniqueKey uk =
        new UniqueKey.Builder("url:string").setDocIdIsUrl(true).build();
    assertEquals("http://localhost/foo/bar", uk.makeUniqueId(rs));
  }

  @Test
  public void testProcessingDocId_docIdIsInvalidUrl() throws Exception {
    executeUpdate("create table data(url varchar)");
    executeUpdate("insert into data(url) values('foo/bar')");

    ResultSet rs = executeQueryAndNext("select * from data");
    UniqueKey uk =
        new UniqueKey.Builder("url:string").setDocIdIsUrl(true).build();
    thrown.expect(URISyntaxException.class);
    uk.makeUniqueId(rs);
  }

  @Test
  public void testProcessingDocIdWithSlash() throws Exception {
    executeUpdate("create table data(a varchar, b varchar)");
    executeUpdate("insert into data(a, b) values('5/5', '6/6')");

    ResultSet rs = executeQueryAndNext("select * from data");
    UniqueKey uk = newUniqueKey("a:string,b:string");
    assertEquals("5_/5/6_/6", uk.makeUniqueId(rs));
  }

  @Test
  public void testProcessingDocIdWithMoreSlashes() throws Exception {
    executeUpdate("create table data(a varchar, b varchar)");
    executeUpdate("insert into data(a, b) values('5/5//', '//6/6')");

    ResultSet rs = executeQueryAndNext("select * from data");
    UniqueKey uk = newUniqueKey("a:string,b:string");
    assertEquals("5_/5_/_/_//_/_/6_/6", uk.makeUniqueId(rs));
  }

  @Test
  public void testPreparingRetrieval_wrongColumnCount() throws SQLException {
    executeUpdate("create table data(id integer, other varchar)");

    String sql = "insert into data(id, other) values (?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk = newUniqueKey("id:int, other:string");
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Wrong number of values for primary key");
      uk.setContentSqlValues(ps, "123/foo/bar");
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

    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals(888, rs.getInt("numnum"));
    assertEquals("bluesky", rs.getString("strstr"));
    assertEquals(new Timestamp(1414701070212L), rs.getTimestamp("timestamp"));
    assertEquals(Date.valueOf("2014-01-01"), rs.getDate("date"));
    assertEquals(Time.valueOf("02:03:04"), rs.getTime("time"));
    assertEquals(123L, rs.getLong("long"));
  }

  @Test
  public void testPreparingRetrievalCaseInsensitiveColumnNames()
      throws SQLException {
    executeUpdate("create table data(numnum int, strstr varchar)");

    String sql = "insert into data(numnum, strstr) values (?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk =
          newUniqueKey("numnum:int, strstr:string", "NUMNUM, StrStr", "");
      uk.setContentSqlValues(ps, "888/bluesky");
      assertEquals(1, ps.executeUpdate());
    }

    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals(888, rs.getInt("numnum"));
    assertEquals("bluesky", rs.getString("strstr"));
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

    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals(888, rs.getInt(1));
    assertEquals(888, rs.getInt(2));
    assertEquals("bluesky", rs.getString(3));
    assertEquals(888, rs.getInt(4));
    assertEquals("bluesky", rs.getString(5));
    assertEquals("bluesky", rs.getString(6));
    assertEquals(888, rs.getInt(7));
  }

  @Test
  public void testPreparingAclRetrievalCaseInsensitiveColumnNames()
      throws SQLException {
    executeUpdate("create table data(numnum int, strstr varchar)");

    String sql = "insert into data(numnum, strstr) values (?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      UniqueKey uk =
          newUniqueKey("numnum:int, strstr:string", "", "NUMNUM, StrStr");
      uk.setAclSqlValues(ps, "888/bluesky");
      assertEquals(1, ps.executeUpdate());
    }

    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals(888, rs.getInt("numnum"));
    assertEquals("bluesky", rs.getString("strstr"));
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

    ResultSet rs = executeQueryAndNext("select * from data");
    assertEquals("5/5", rs.getString(1));
    assertEquals("6/6", rs.getString(2));
    assertEquals("5/5", rs.getString(3));
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
      throws Exception {
    try {
      executeUpdate("create table data(a varchar, b varchar)");
      executeUpdate(
          "insert into data(a, b) values('" + elem1 + "', '" + elem2 + "')");

      UniqueKey uk = newUniqueKey("a:string,b:string");
      ResultSet rs = executeQueryAndNext("select * from data");
      String id = uk.makeUniqueId(rs);

      String sql = "select * from data where a = ? and b = ?";
      try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
        uk.setContentSqlValues(ps, id);
        rs = executeQueryAndNext("select * from data");
        assertEquals(elem1, rs.getString(1));
        assertEquals(elem2, rs.getString(2));
      } catch (Exception e) {
        throw new RuntimeException("elem1: " + elem1 + ", elem2: " + elem2 
            + ", id: " + id, e);
      }
    } finally {
      dropAllObjects();
    }
  }

  @Test
  public void testFuzzSlashesAndEscapes() throws Exception {
    for (int fuzzCase = 0; fuzzCase < 1000; fuzzCase++) {
      String elem1 = makeSomeIdsWithJustSlashesAndEscapeChar();
      String elem2 = makeSomeIdsWithJustSlashesAndEscapeChar();
      testUniqueElementsRoundTrip(elem1, elem2);
    }
  }

  @Test
  public void testEmptiesPreserved() throws Exception {
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
  public void testSpacesInUniqueKeyDeclerations() {
    UniqueKey.Builder builder = new UniqueKey.Builder("numnum:int,other:int");
    List<String> goldenNames = builder.getDocIdSqlColumns();
    Map<String, ColumnType> goldenTypes = builder.getColumnTypes();

    List<String> testDecls = asList(" numnum:int,other:int",
        "numnum:int ,other:int", "numnum:int, other:int",
        "numnum:int , other:int", "   numnum:int  ,  other:int   ",
        "numnum:int,other:int", "numnum: int,other:int",
        "numnum : int,other:int", "numnum : int,other   :    int");
    for (String ukDecl : testDecls) {
      UniqueKey.Builder testBuilder = new UniqueKey.Builder(ukDecl);
      assertEquals(goldenNames, testBuilder.getDocIdSqlColumns());
      assertEquals(goldenTypes, testBuilder.getColumnTypes());
    }
  }

  @Test
  public void testSpacesBetweenContentSqlCols() {
    String ukDecl = "numnum:int,strstr:string";
    UniqueKey.Builder builder = new UniqueKey.Builder(ukDecl)
        .setContentSqlColumns("numnum,numnum,strstr,numnum,strstr");
    List<String> goldenNames = builder.getContentSqlColumns();

    List<String> testDecls = asList(
        "numnum ,numnum,strstr,numnum,strstr",
        "numnum, numnum,strstr,numnum,strstr",
        "numnum , numnum,strstr,numnum,strstr",
        "numnum  ,   numnum,strstr,numnum,strstr",
        "numnum  ,   numnum , strstr   ,  numnum,strstr");
    for (String colDecl : testDecls) {
      UniqueKey.Builder testBuilder = new UniqueKey.Builder(ukDecl)
          .setContentSqlColumns(colDecl);
      assertEquals(goldenNames, testBuilder.getContentSqlColumns());
    }
  }

  @Test
  public void testSpacesBetweenAclSqlCols() {
    String ukDecl = "numnum:int,strstr:string";
    UniqueKey.Builder builder = new UniqueKey.Builder(ukDecl)
        .setAclSqlColumns("numnum,numnum,strstr,numnum,strstr");
    List<String> goldenNames = builder.getAclSqlColumns();

    List<String> testDecls = asList(
        "numnum ,numnum,strstr,numnum,strstr",
        "numnum, numnum,strstr,numnum,strstr",
        "numnum , numnum,strstr,numnum,strstr",
        "numnum  ,   numnum,strstr,numnum,strstr",
        "numnum  ,   numnum , strstr   ,  numnum,strstr");
    for (String colDecl : testDecls) {
      UniqueKey.Builder testBuilder = new UniqueKey.Builder(ukDecl)
          .setAclSqlColumns(colDecl);
      assertEquals(goldenNames, testBuilder.getAclSqlColumns());
    }
  }
}
