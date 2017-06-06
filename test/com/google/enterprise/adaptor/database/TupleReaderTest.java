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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
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
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullVARCHAR() throws Exception {
    executeUpdate("create table data(id integer, colname varchar)");
    executeUpdate("insert into data(id) values(1)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"VARCHAR\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test Char column.
   */
  @Test
  public void testChar() throws Exception {
    executeUpdate("create table data(colname char)");
    executeUpdate("insert into data(colname) values('onevalue')");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"CHAR\">onevalue</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullChar() throws Exception {
    executeUpdate("create table data(id integer, colname char)");
    executeUpdate("insert into data(id) values(1)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"CHAR\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test integer column.
   */
  @Test
  public void testINTEGER() throws Exception {
    executeUpdate("create table data(colname integer)");
    executeUpdate("insert into data(colname) values(17)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"INTEGER\">17</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullINTEGER() throws Exception {
    executeUpdate("create table data(id integer, colname integer)");
    executeUpdate("insert into data(id) values(1)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"INTEGER\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test date column.
   */
  @Test
  public void testDATE() throws Exception {
    executeUpdate("create table data(colname date)");
    executeUpdate("insert into data(colname) values({d '2004-10-06'})");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"DATE\">2004-10-06</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullDATE() throws Exception {
    executeUpdate("create table data(id integer, colname date)");
    executeUpdate("insert into data(id) values(1)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"DATE\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test TIMESTAMP column.
   */
  @Test
  public void testTIMESTAMP() throws Exception {
    executeUpdate("create table data(colname timestamp)");
    executeUpdate(
        "insert into data(colname) values ({ts '2004-10-06T09:15:30'})");
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
        + "<COLNAME SQLType=\"TIMESTAMP\">2004-10-06T09:15:30"
        + timeZoneFmt.format(date) + ":00"
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullTIMESTAMP() throws Exception {
    executeUpdate("create table data(id integer, colname timestamp)");
    executeUpdate("insert into data(id) values(1)");
    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    cal.set(Calendar.YEAR, 2004);
    cal.set(Calendar.MONTH, Calendar.OCTOBER);
    cal.set(Calendar.DATE, 6);
    cal.set(Calendar.HOUR, 9);
    cal.set(Calendar.MINUTE, 15);
    cal.set(Calendar.SECOND, 30);
    String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"TIMESTAMP\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /**
   * Test TIME column.
   */
  @Test
  public void testTIME() throws Exception {
    executeUpdate("create table data(colname time)");
    executeUpdate("insert into data(colname) values({t '09:15:30'})");
    // H2 returns a java.sql.Date with the date set to 1970-01-01.
    final DateFormat timeZoneFmt = new SimpleDateFormat("X");
    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    cal.set(Calendar.YEAR, 1970);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.DATE, 1);
    cal.set(Calendar.HOUR, 9);
    cal.set(Calendar.MINUTE, 15);
    cal.set(Calendar.SECOND, 30);
    Date date = cal.getTime();
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"TIME\">09:15:30"
        + timeZoneFmt.format(date) + ":00"
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullTIME() throws Exception {
    executeUpdate("create table data(id integer, colname time)");
    executeUpdate("insert into data(id) values(1)");
    // H2 returns a java.sql.Date with the date set to 1970-01-01.
    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    cal.set(Calendar.YEAR, 1970);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.DATE, 1);
    cal.set(Calendar.HOUR, 9);
    cal.set(Calendar.MINUTE, 15);
    cal.set(Calendar.SECOND, 30);
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"TIME\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
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
    executeUpdate("create table data(colname blob)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBinaryStream(1, new ByteArrayInputStream(blobData.getBytes(UTF_8)));
      assertEquals(1, ps.executeUpdate());
    }
    String base64BlobData = DatatypeConverter.printBase64Binary(
        blobData.getBytes(UTF_8));
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"BLOB\" encoding=\"base64binary\">"
        + base64BlobData
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testNullBLOB() throws Exception {
    executeUpdate("create table data(id integer, colname blob)");
    executeUpdate("insert into data(id) values(1)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<COLNAME SQLType=\"BLOB\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
}
