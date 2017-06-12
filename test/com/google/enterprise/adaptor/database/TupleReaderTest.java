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

import static com.google.enterprise.adaptor.database.JdbcFixture.executeQuery;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
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

  private Transformer perTestTransformer;

  @Before
  public void setUp() throws TransformerConfigurationException {
    perTestTransformer = transformer();
  }

  private Transformer transformer() throws TransformerConfigurationException {
    TransformerFactory transFactory = TransformerFactory.newInstance();
    // Create a new Transformer that performs a copy of the Source to the
    // Result. i.e. the "identity transform".
    Transformer trans = transFactory.newTransformer();
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    return trans;
  }

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  private String generateXml(ResultSet rs) throws TransformerException {
    return generateXml(perTestTransformer, rs);
  }

  private String generateXml(Transformer trans, ResultSet rs)
      throws TransformerException {
    TupleReader reader = new TupleReader(rs);
    Source source = new SAXSource(reader, /*ignored*/new InputSource());
    StringWriter writer = new StringWriter();
    Result des = new StreamResult(writer);
    trans.transform(source, des);
    return writer.toString();
  }

  @Test
  public void testConstructor_nullResultSet() {
    thrown.expect(NullPointerException.class);
    new TupleReader(null);
  }

  @Test
  public void testParse_emptyResultSet() throws Exception {
    executeUpdate("create table data(colname varchar)");
    ResultSet rs = executeQuery("select * from data");
    thrown.expect(TransformerException.class);
    thrown.expectCause(isA(SAXException.class));
    thrown.expectMessage("No data is available");
    generateXml(rs);
  }

  @Test
  public void testVarchar() throws Exception {
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
  public void testVarchar_xml() throws Exception {
    final String template = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"VARCHAR\">%s</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    String xml = String.format(template, "onevalue");
    executeUpdate("create table data(colname varchar)");
    executeUpdate("insert into data(colname) values('" + xml + "')");
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(
        String.format(template, xml.replace("<", "&lt;").replace(">", "&gt;")),
        result);
  }

  @Test
  public void testVarchar_null() throws Exception {
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
  public void testChar_null() throws Exception {
    executeUpdate("create table data(colname char)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"CHAR\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testInteger() throws Exception {
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
  public void testInteger_null() throws Exception {
    executeUpdate("create table data(colname integer)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"INTEGER\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testBoolean() throws Exception {
    executeUpdate("create table data(colname boolean)");
    executeUpdate("insert into data(colname) values(true)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"BOOLEAN\">true</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testBoolean_null() throws Exception {
    executeUpdate("create table data(colname boolean)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"BOOLEAN\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testDate() throws Exception {
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
  public void testDate_null() throws Exception {
    executeUpdate("create table data(colname date)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"DATE\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testTimestamp() throws Exception {
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
  public void testTimestamp_threadSafe() throws Throwable {
    // Without ThreadLocal, even 2 threads and 1 iteration would throw
    // an exception. These values are still very quick to test
    // (14 milliseconds in my testing).
    final int threadCount = 5;
    final int iterations = 20;

    // We're going to generate dates randomly distributed around the
    // current date (e.g., in 2013, between 1970 and 2056).
    Random rnd = new Random();
    long now = new Date().getTime();
    long scale = 2 * now / Integer.MAX_VALUE;

    Thread[] threads = new Thread[threadCount];
    final Throwable[] errors = new Throwable[threadCount];

    executeUpdate("create table data(thread int, colname timestamp)");

    // Start the threads.
    for (int t = 0; t < threadCount; t++) {
      long time = rnd.nextInt(Integer.MAX_VALUE) * scale;

      Timestamp ts = new Timestamp(time);
      executeUpdate("insert into data(thread, colname) values ("
          + t + ", {ts '" + ts + "'})");

      DateFormat timeZoneFmt = new SimpleDateFormat("X");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time);
      Date date = cal.getTime();
      final String golden = ""
          + "<database>"
          + "<table>"
          + "<table_rec>"
          + "<COLNAME SQLType=\"TIMESTAMP\">"
          + ts.toString().replace(' ', 'T').replaceFirst("\\.\\d+$", "")
          + timeZoneFmt.format(date) + ":00"
          + "</COLNAME>"
          + "</table_rec>"
          + "</table>"
          + "</database>";

      final Transformer perThreadTransformer = transformer();

      // Collect errors in the threads to throw them from the test
      // thread or the test won't fail.
      final int tt = t;
      threads[t] = new Thread() {
          @Override public void run() {
            try {
              ResultSet rs = executeQueryAndNext(
                  "select colname from data where thread = " + tt);
              for (int i = 0; i < iterations; i++) {
                String result = generateXml(perThreadTransformer, rs);
                assertEquals("Thread " + tt + "; iteration " + i,
                    golden, result);
              }
            } catch (Throwable e) {
              errors[tt] = e;
            }
          }
        };
      threads[t].start();
    }

    // Wait for each thread and check for errors.
    StringBuilder builder = new StringBuilder();
    for (int t = 0; t < threadCount; t++) {
      threads[t].join();
      if (errors[t] != null) {
        builder.append(errors[t]).append("\n");
      }
    }
    assertEquals(builder.toString(), 0, builder.length());
  }

  @Test
  public void testTimestamp_null() throws Exception {
    executeUpdate("create table data(colname timestamp)");
    executeUpdate("insert into data(colname) values(null)");
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
        + "<COLNAME SQLType=\"TIMESTAMP\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testTime() throws Exception {
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
  public void testTime_null() throws Exception {
    executeUpdate("create table data(colname time)");
    executeUpdate("insert into data(colname) values(null)");
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
        + "<COLNAME SQLType=\"TIME\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testBlob() throws Exception {
    byte[] blobData = new byte[12345];
    new Random().nextBytes(blobData);
    executeUpdate("create table data(colname blob)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBinaryStream(1, new ByteArrayInputStream(blobData));
      assertEquals(1, ps.executeUpdate());
    }
    String base64BlobData = DatatypeConverter.printBase64Binary(blobData);
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
  public void testBlob_empty() throws Exception {
    executeUpdate("create table data(colname blob)");
    executeUpdate("insert into data(colname) values('')");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"BLOB\" encoding=\"base64binary\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testBlob_null() throws Exception {
    executeUpdate("create table data(colname blob)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"BLOB\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testClob() throws Exception {
    String clobData =
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
    executeUpdate("create table data(colname clob)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, clobData);
      assertEquals(1, ps.executeUpdate());
    }
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"CLOB\">"
        + clobData
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testClob_null() throws Exception {
    executeUpdate("create table data(colname clob)");
    executeUpdate("insert into data(colname) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"CLOB\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testArray() throws Exception {
    String[] array = { "hello", "world" };
    executeUpdate("create table data(colname array)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setObject(1, array);
      assertEquals(1, ps.executeUpdate());
    }
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec/>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testArray_null() throws Exception {
    executeUpdate("create table data(other array)");
    executeUpdate("insert into data(other) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec/>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testOther() throws Exception {
    // H2's OTHER type requires a serialized Java object.
    String serializable = "hello world";
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(serializable);
    }

    executeUpdate("create table data(colname other)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setObject(1, buffer.toByteArray());
      assertEquals(1, ps.executeUpdate());
    }
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"OTHER\">"
        + serializable
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testOther_null() throws Exception {
    executeUpdate("create table data(other other)");
    executeUpdate("insert into data(other) values(null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<OTHER SQLType=\"OTHER\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testMultipleTypes() throws Exception {
    executeUpdate("create table data(id integer, name varchar, modified date)");
    executeUpdate(
        "insert into data(id, name, modified) values(1, 'file.txt', null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\"INTEGER\">1</ID>"
        + "<NAME SQLType=\"VARCHAR\">file.txt</NAME>"
        + "<MODIFIED SQLType=\"DATE\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testInvalidXmlChars() throws Exception {
    StringBuilder input = new StringBuilder();
    StringBuilder output = new StringBuilder();
    for (char i = '\0'; i <= '\u001F'; i++) {
      input.append(i);
      output.append('\uFFFD');
    }
    input.append("\uFFFE\uFFFF");
    output.append("\uFFFD\uFFFD");
    output.setCharAt('\t', '\t');
    output.setCharAt('\n', '\n');
    output.replace('\r', '\r' + 1, "&#13;");

    executeUpdate("create table data(colname varchar)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, input.toString());
      assertEquals(1, ps.executeUpdate());
    }
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"VARCHAR\">"
        + output.toString()
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  /** Test all Unicode BMP characters, plus some surrogate pairs. */
  @Test
  public void testValidXmlChars() throws Exception {
    StringBuilder buf = new StringBuilder(10000000);
    for (int i = 0x20; i <= 0xD7FF; i++) {
      buf.append(Character.toChars(i));
    }
    for (int i = 0xE000; i <= 0xFFFD; i++) {
      buf.append(Character.toChars(i));
    }
    // Include some surrogate pairs from musical symbols.
    StringBuilder surrogates = new StringBuilder();
    for (int i = 0x1D100; i < 0x1D108; i++) {
      surrogates.append(Character.toChars(i));
    }
    assertEquals(16, surrogates.length());
    buf.append(surrogates);
    String content = buf.toString();

    executeUpdate("create table data(colname varchar)");
    String sql = "insert into data(colname) values (?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\"VARCHAR\">"
        + content
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace("\u007F", "&#127;")
        .replace("\u0080\u0081\u0082\u0083\u0084\u0085\u0086\u0087",
          "&#128;&#129;&#130;&#131;&#132;&#133;&#134;&#135;")
        .replace("\u0088\u0089\u008A\u008B\u008C\u008D\u008E\u008F",
          "&#136;&#137;&#138;&#139;&#140;&#141;&#142;&#143;")
        .replace("\u0090\u0091\u0092\u0093\u0094\u0095\u0096\u0097",
          "&#144;&#145;&#146;&#147;&#148;&#149;&#150;&#151;")
        .replace("\u0098\u0099\u009A\u009B\u009C\u009D\u009E\u009F",
            "&#152;&#153;&#154;&#155;&#156;&#157;&#158;&#159;")
        .replace(surrogates.toString(),
            "&#119040;&#119041;&#119042;&#119043;"
            + "&#119044;&#119045;&#119046;&#119047;")
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }
}
