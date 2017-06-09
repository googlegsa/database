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
import static com.google.enterprise.adaptor.database.UniqueKey.ColumnType;
import static java.util.Arrays.asList;
import static java.util.Locale.US;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
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
    UniqueKey.Builder ukb = new UniqueKey.Builder("numnum:int");
    assertEquals(asList("numnum"), ukb.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, ukb.getColumnTypes().get("numnum"));
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
    UniqueKey.Builder ukb = new UniqueKey.Builder("numnum:int,other:int");
    assertEquals(asList("numnum", "other"), ukb.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, ukb.getColumnTypes().get("numnum"));
    assertEquals(ColumnType.INT, ukb.getColumnTypes().get("other"));
  }

  @Test
  public void testIntString() {
    UniqueKey.Builder ukb = new UniqueKey.Builder("numnum:int,strstr:string");
    assertEquals(asList("numnum", "strstr"), ukb.getDocIdSqlColumns());
    assertEquals(ColumnType.INT, ukb.getColumnTypes().get("numnum"));
    assertEquals(ColumnType.STRING, ukb.getColumnTypes().get("strstr"));
  }

  @Test
  public void testUrlIntColumn() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    new UniqueKey.Builder("numnum:int").setEncodeDocIds(false).build();
  }

  @Test
  public void testUrlTwoColumns() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    new UniqueKey.Builder("str1:string,str2:string")
        .setEncodeDocIds(false).build();
  }

  @Test
  public void testUrlColumnMissingType() {
    thrown.expect(InvalidConfigurationException.class);
    new UniqueKey.Builder("url").setEncodeDocIds(false).build();
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

    UniqueKey.Builder ukb = new UniqueKey.Builder("BIT, BOOLEAN, TINYINT, "
        + "SMALLINT, INTEGER, BIGINT, CHAR, VARCHAR, LONGVARCHAR, NCHAR, "
        + "NVARCHAR, LONGNVARCHAR, DATALINK, DATE, TIME, TIMESTAMP");
    ukb.addColumnTypes(sqlTypes);
    assertEquals(golden, ukb.getColumnTypes());
  }

  @Test
  public void testAddColumnTypes_invalidType() {
    Map<String, Integer> sqlTypes = new HashMap<>();
    sqlTypes.put("blob", Types.BLOB);

    UniqueKey.Builder ukb = new UniqueKey.Builder("blob");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey SQLtype");
    ukb.addColumnTypes(sqlTypes);
  }

  @Test
  public void testEquals() throws Exception {
    UniqueKey uk1 = new UniqueKey.Builder("id:int").build();
    assertTrue(uk1.equals(new UniqueKey.Builder("id:int").build()));
    assertFalse(uk1.equals(new UniqueKey.Builder("foo:int").build()));
    assertFalse(uk1.equals(new UniqueKey.Builder("id:string").build()));
    UniqueKey uk2 = new UniqueKey.Builder("id:int, other:string").build();
    assertFalse(uk2.equals(uk1));
    assertFalse(uk2.equals(new UniqueKey.Builder("id:int, other:string")
        .setContentSqlColumns("other").build()));
    assertFalse(uk2.equals(new UniqueKey.Builder("id:int, other:string")
        .setAclSqlColumns("other").build()));
    assertFalse(uk1.equals("Foo"));
  }

  @Test
  public void testEqualsHashCode() throws Exception {
    int hashCode1 =  new UniqueKey.Builder("id:int").build().hashCode();
    assertEquals(hashCode1, new UniqueKey.Builder("id:int").build().hashCode());
    assertFalse(
        hashCode1 == new UniqueKey.Builder("foo:int").build().hashCode());
    assertFalse(
        hashCode1 == new UniqueKey.Builder("id:string").build().hashCode());
    int hashCode2
        = new UniqueKey.Builder("id:int, other:string").build().hashCode();
    assertFalse(hashCode2 == hashCode1);
    assertFalse(hashCode2 == new UniqueKey.Builder("id:int, other:string")
        .setContentSqlColumns("other").build().hashCode());
    assertFalse(hashCode2 == new UniqueKey.Builder("id:int, other:string")
        .setAclSqlColumns("other").build().hashCode());
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
  public void testProcessingDocId_encodingError() throws SQLException {
    executeUpdate("create table data(numnum int, strstr varchar)");
    executeUpdate("insert into data(numnum, strstr) values(345, 'abc')");

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      UniqueKey uk = newUniqueKey("numnum:int,strstr:string");
      assertTrue("ResultSet is empty", rs.next());
      thrown.handleAssertionErrors();
      thrown.expect(AssertionError.class);
      thrown.expectMessage("not encoding implies exactly one parameter");
      uk.makeUniqueId(rs, /*encode=*/ false);
    }
  }

  @Test
  public void testProcessingDocId_allTypes() throws SQLException {
    executeUpdate("create table data(c1 integer, c2 bigint, c3 varchar, "
        + "c4 date, c5 time, c6 timestamp)");
    String sql = "insert into data(c1, c2, c3, c4, c5, c6) "
        + "values (?, ?, ?, ?, ?, ?)";
    Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("GMT"), US);
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setInt(1, 123);
      ps.setLong(2, 4567890L);
      ps.setString(3, "foo");
      ps.setDate(4, new Date(0L), gmt);
      ps.setTime(5, new Time(0L), gmt);
      // TODO(bmj): setTimestamp with Calendar broken in H2?
      ps.setTimestamp(6, new Timestamp(0L)/*, gmt*/);
      assertEquals(1, ps.executeUpdate());
    }

    UniqueKey uk = new UniqueKey.Builder(
        "c1:int, c2:long, c3:string, c4:date, c5:time, c6:timestamp").build();

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      assertEquals("123/4567890/foo/1970-01-01/00:00:00/0",
          uk.makeUniqueId(rs, /*encode=*/ true));
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
