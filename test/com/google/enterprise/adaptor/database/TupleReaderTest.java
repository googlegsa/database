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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

/** Unit Test for {@link TupleReader}. */
public class TupleReaderTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  
  private final TransformerFactory transFactory;
  private final Transformer trans;
  
  public TupleReaderTest() throws TransformerConfigurationException {
    transFactory = TransformerFactory.newInstance();
    // Create a new Transformer that performs a copy of the Source to the
    // Result. i.e. the "identity transform".
    trans = transFactory.newTransformer();
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
  }

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  private String generateXml(ResultSet rs) throws TransformerException {
    TupleReader reader = new TupleReader(rs);
    Source source = new SAXSource(reader, /*ignored*/new InputSource());
    StringWriter writer = new StringWriter();
    Result des = new StreamResult(writer);
    trans.transform(source, des);
    return writer.toString();
  }
  
  /**
   * Test null value uses "ISNULL" attribute.
   */
  @Test
  public void testNULL() throws Exception {
    executeUpdate("create table data(colname varchar)");
    executeUpdate("insert into data(colname) values(null)");

    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"VARCHAR\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      String result = generateXml(rs);
      assertEquals(golden, result);
    }
  }

  /**
   * Test varchar column.
   */
  @Test
  public void testVARCHAR() throws Exception {
    executeUpdate("create table data(colname varchar)");
    executeUpdate("insert into data(colname) values('onevalue')");

    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"VARCHAR\">onevalue</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from data")) {
      assertTrue("ResultSet is empty", rs.next());
      String result = generateXml(rs);
      assertEquals(golden, result);
    }
  }

  /**
   * Test Char column.
   */
  @Test
  public void testChar() throws Exception {
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"CHAR\">onevalue</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.CHAR};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    MockResultSet resultSet = new MockResultSet(rsMetadata, "onevalue");
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test integer column.
   */
  @Test
  public void testINTEGER() throws Exception {
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"INTEGER\">17</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.INTEGER};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    MockResultSet resultSet = new MockResultSet(rsMetadata, "17");
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
  
  /**
   * Test date column.
   */
  @Test
  public void testDATE() throws Exception {
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"DATE\">2004-10-06</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.DATE};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, 2004);
    cal.set(Calendar.MONTH, Calendar.OCTOBER);
    cal.set(Calendar.DATE, 6);
    java.sql.Date retDate = new java.sql.Date(cal.getTime().getTime());
    MockResultSet resultSet = new MockResultSet(rsMetadata, retDate);
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
  
  /**
   * Test TIMESTAMP column.
   */
  @Test
  public void testTIMESTAMP() throws Exception {
    DateFormat timeZoneFmt = new SimpleDateFormat("X");
    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    cal.set(Calendar.YEAR, 2004);
    cal.set(Calendar.MONTH, Calendar.OCTOBER);
    cal.set(Calendar.DATE, 6);
    cal.set(Calendar.HOUR, 9);
    cal.set(Calendar.MINUTE, 15);
    cal.set(Calendar.SECOND, 30);
    Date date = cal.getTime();
    String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"TIMESTAMP\">2004-10-06T09:15:30"
        + timeZoneFmt.format(date) + ":00"
        + "</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.TIMESTAMP};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    long gmtTime = cal.getTime().getTime();
    java.sql.Timestamp retDate = new java.sql.Timestamp(gmtTime);
    MockResultSet resultSet = new MockResultSet(rsMetadata, retDate);
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test TIME column.
   */
  @Test
  public void testTIME() throws Exception {
    final DateFormat timeZoneFmt = new SimpleDateFormat("X");
    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    cal.set(Calendar.YEAR, 2004);
    cal.set(Calendar.MONTH, Calendar.OCTOBER);
    cal.set(Calendar.DATE, 6);
    cal.set(Calendar.HOUR, 9);
    cal.set(Calendar.MINUTE, 15);
    cal.set(Calendar.SECOND, 30);
    Date date = cal.getTime();
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"TIME\">09:15:30"
        + timeZoneFmt.format(date) + ":00"
        + "</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.TIME};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    long gmtTime = cal.getTime().getTime();
    java.sql.Time retDate = new java.sql.Time(gmtTime);
    MockResultSet resultSet = new MockResultSet(rsMetadata, retDate);
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
  
  /**
   * Test BLOB column.
   */
  @Test
  public void testBLOB() throws Exception {
    String blobData =
        " Google's indices consist of information that has been"
            + " identified, indexed and compiled through an automated"
            + " process with no advance review by human beings. Given"
            + " the enormous volume of web site information added,"
            + " deleted, and changed on a frequent basis, Google cannot"
            + " and does not screen anything made available through its"
            + " indices. For each web site reflected in Google's"
            + " indices, if either (i) a site owner restricts access to"
            + " his or her web site or (ii) a site is taken down from"
            + " the web, then, upon receipt of a request by the site"
            + " owner or a third party in the second instance, Google"
            + " would consider on a case-by-case basis requests to"
            + " remove the link to that site from its indices. However,"
            + " if the operator of the site does not take steps to"
            + " prevent it, the automatic facilities used to create"
            + " the indices are likely to find that site and index it"
            + " again in a relatively short amount of time.";
    String base64BlobData = DatatypeConverter.printBase64Binary(
        blobData.getBytes());
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<colname SQLType=\"LONGVARBINARY\" encoding=\"base64binary\">"
        + base64BlobData
        + "</colname>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    int[] columnType = {Types.LONGVARBINARY};
    String[] columnName = {"colname"};
    MockResultSetMetaData metadata =
        new MockResultSetMetaData(columnType, columnName);
    ResultSetMetaData rsMetadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, metadata);
    MockResultSet resultSet = new MockResultSet(rsMetadata,
        new ByteArrayInputStream(blobData.getBytes()));
    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, resultSet);
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
  
  private static class MockResultSet implements InvocationHandler {
    private ResultSetMetaData metadata;
    private Object sqlObjectValue;
    public MockResultSet(ResultSetMetaData metadata, Object value) {
      this.metadata = metadata;
      this.sqlObjectValue = value;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName(); 
      if ("getMetaData".equals(methodName)) {
        return metadata;
      } else if ("getDate".equals(methodName)) {
        return (Date) sqlObjectValue;
      } else if ("getTimestamp".equals(methodName)) {
        return (Timestamp) sqlObjectValue;
      } else if ("getTime".equals(methodName)) {
        return (Time) sqlObjectValue;
      } else if ("getBinaryStream".equals(methodName)) {
        return (InputStream) sqlObjectValue;
      } else if ("getString".equals(methodName)) {
        return (String) sqlObjectValue;
      } else if ("getCharacterStream".equals(methodName)) {
        return (Reader) sqlObjectValue;
      } else if ("getObject".equals(methodName)) {
        return sqlObjectValue;
      } else {
        throw new AssertionError("unexpected call: " + methodName);
      }
    }
  }
  
  private static class MockResultSetMetaData implements InvocationHandler {
    private int[] columnType;
    private String[] columnName;
    
    public MockResultSetMetaData(int[] columnType, String[] columnName) {
      assertNotNull(columnType);
      assertNotNull(columnName);
      assertEquals(columnType.length, columnName.length);
      this.columnType = columnType;
      this.columnName = columnName;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName();
      if ("getColumnCount".equals(methodName)) {
        return columnType.length;
      } else if ("getColumnLabel".equals(methodName)) {
        return columnName[(Integer) args[0] - 1];
      } else if ("getColumnType".equals(methodName)) {
        return columnType[(Integer) args[0] - 1];
      } else {
        throw new AssertionError("unexpected call: " + methodName);
      }
    }
  }
}
