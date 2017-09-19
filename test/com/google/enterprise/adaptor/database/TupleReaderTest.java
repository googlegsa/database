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

import static com.google.enterprise.adaptor.database.JdbcFixture.Database.H2;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.MYSQL;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.ORACLE;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.SQLSERVER;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQuery;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static com.google.enterprise.adaptor.database.JdbcFixture.is;
import static com.google.enterprise.adaptor.database.JdbcFixture.prepareStatement;
import static com.google.enterprise.adaptor.database.JdbcFixture.setObject;
import static java.util.Locale.US;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
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
    executeUpdate("create table data(colname varchar(20))");
    ResultSet rs = executeQuery("select * from data");
    thrown.expect(TransformerException.class);
    thrown.expectCause(isA(SAXException.class));
    generateXml(rs);
  }

  /** @see testGenerateXml(String, int, String, Object, String) */
  private void testGenerateXml(String columnName, int sqlType, Object input,
      String output) throws Exception {
    testGenerateXml(columnName, sqlType,
        DatabaseAdaptor.getColumnTypeName(sqlType, null, 0).toLowerCase(US),
        input, output);
  }

  /**
   * Parameterized test for TupleReader using different SQL data types.
   *
   * @param columnName the column name
   * @param sqlType the SQL data type
   * @param sqlTypeDecl the SQL data type declaration
   * @param input the SQL value inserted into the database
   * @param output the expected content stream value
   */
  private void testGenerateXml(String columnName, int sqlType,
      String sqlTypeDecl, Object input, String output)
      throws SQLException, TransformerException {
    executeUpdate("create table data(" + columnName + " " + sqlTypeDecl + ")");
    String sql = "insert into data(" + columnName + ") values (?)";
    PreparedStatement ps = prepareStatement(sql);
    setObject(ps, 1, input, sqlType);
    assertEquals(1, ps.executeUpdate());

    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + output
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testInteger() throws Exception {
    testGenerateXml("COLNAME", Types.INTEGER, 17,
        "<COLNAME SQLType=\"" + JdbcFixture.INTEGER + "\">17</COLNAME>");
  }

  @Test
  public void testInteger_null() throws Exception {
    testGenerateXml("COLNAME", Types.INTEGER, null,
        "<COLNAME SQLType=\"" + JdbcFixture.INTEGER + "\" ISNULL=\"true\"/>");
  }

  @Test
  public void testBoolean() throws Exception {
    assumeFalse("BOOLEAN type not supported", is(ORACLE));
    testGenerateXml("COLNAME", Types.BOOLEAN, JdbcFixture.BOOLEAN, true,
        "<COLNAME SQLType=\"" + JdbcFixture.BOOLEAN + "\">true</COLNAME>");
  }

  @Test
  public void testBoolean_null() throws Exception {
    assumeFalse("BOOLEAN type not supported", is(ORACLE));
    testGenerateXml("COLNAME", Types.BOOLEAN, JdbcFixture.BOOLEAN, null,
        "<COLNAME SQLType=\"" + JdbcFixture.BOOLEAN + "\" ISNULL=\"true\"/>");
  }

  @Test
  public void testChar() throws Exception {
    testGenerateXml("COLNAME", Types.CHAR, "char(20)", "onevalue",
        "<COLNAME SQLType=\"CHAR\">"
        + (is(ORACLE) || is(SQLSERVER) ? "onevalue            " : "onevalue")
        + "</COLNAME>");
  }

  @Test
  public void testChar_null() throws Exception {
    testGenerateXml("COLNAME", Types.CHAR, null,
        "<COLNAME SQLType=\"CHAR\" ISNULL=\"true\"/>");
  }

  @Test
  public void testVarchar() throws Exception {
    testGenerateXml("COLNAME", Types.VARCHAR, "varchar(20)", "onevalue",
        "<COLNAME SQLType=\"VARCHAR\">onevalue</COLNAME>");
  }

  @Test
  public void testVarchar_xml() throws Exception {
    String template = "<COLNAME SQLType=\"VARCHAR\">%s</COLNAME>";
    String xml = String.format(template, "onevalue");
    testGenerateXml("COLNAME", Types.VARCHAR, "varchar(200)", xml,
        String.format(template, xml.replace("<", "&lt;").replace(">", "&gt;")));
  }

  @Test
  public void testVarchar_null() throws Exception {
    testGenerateXml("COLNAME", Types.VARCHAR, "varchar(20)", null,
        "<COLNAME SQLType=\"VARCHAR\" ISNULL=\"true\"/>");
  }

  @Test
  public void testLongvarchar() throws Exception {
    assumeFalse("LONGVARCHAR type not supported", is(H2));
    testGenerateXml("COLNAME", Types.LONGVARCHAR, "onevalue",
        "<COLNAME SQLType=\"LONGVARCHAR\">onevalue</COLNAME>");
  }

  @Test
  public void testLongvarchar_null() throws Exception {
    assumeFalse("LONGVARCHAR type not supported", is(H2));
    testGenerateXml("COLNAME", Types.LONGVARCHAR, null,
        "<COLNAME SQLType=\"LONGVARCHAR\" ISNULL=\"true\"/>");
  }

  @Test
  public void testBinary() throws Exception {
    byte[] binaryData = new byte[123];
    new Random().nextBytes(binaryData);
    String base64BinaryData = DatatypeConverter.printBase64Binary(binaryData);
    testGenerateXml("COLNAME", Types.VARBINARY, "varbinary(200)", binaryData,
        "<COLNAME SQLType=\"VARBINARY\" encoding=\"base64binary\">"
        + base64BinaryData
        + "</COLNAME>");
  }

  @Test
  public void testBinary_empty() throws Exception {
    byte[] binaryData = new byte[0];
    testGenerateXml("COLNAME", Types.VARBINARY, "varbinary(1)", binaryData,
        "<COLNAME SQLType=\"VARBINARY\" "
        + (is(ORACLE) ? "ISNULL=\"true\"" : "encoding=\"base64binary\"")
        + "/>");
  }

  @Test
  public void testBinary_null() throws Exception {
    testGenerateXml("COLNAME", Types.VARBINARY, "varbinary(20)", null,
        "<COLNAME SQLType=\"VARBINARY\" ISNULL=\"true\"/>");
  }

  @Test
  public void testLongvarbinary() throws Exception {
    assumeFalse("LONGVARBINARY type not supported", is(H2));
    byte[] longvarbinaryData = new byte[12345];
    new Random().nextBytes(longvarbinaryData);
    String base64LongvarbinaryData =
        DatatypeConverter.printBase64Binary(longvarbinaryData);
    testGenerateXml("COLNAME", Types.LONGVARBINARY, longvarbinaryData,
        "<COLNAME SQLType=\"LONGVARBINARY\" encoding=\"base64binary\">"
        + base64LongvarbinaryData
        + "</COLNAME>");
  }

  @Test
  public void testLongvarbinary_empty() throws Exception {
    assumeFalse("LONGVARBINARY type not supported", is(H2));
    byte[] emptyData = {};
    testGenerateXml("COLNAME", Types.LONGVARBINARY, emptyData,
        "<COLNAME SQLType=\"LONGVARBINARY\" "
        + (is(ORACLE) ? "ISNULL=\"true\"" : "encoding=\"base64binary\"")
        + "/>");
  }

  @Test
  public void testLongvarbinary_null() throws Exception {
    assumeFalse("LONGVARBINARY type not supported", is(H2));
    testGenerateXml("COLNAME", Types.LONGVARBINARY, (byte[]) null,
        "<COLNAME SQLType=\"LONGVARBINARY\" ISNULL=\"true\"/>");
  }

  @Test
  public void testDate() throws Exception {
    assumeFalse("DATE type not supported", is(ORACLE));
    executeUpdate("create table data(COLNAME date)");
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
    assumeFalse("DATE type not supported", is(ORACLE));
    testGenerateXml("COLNAME", Types.DATE, null,
        "<COLNAME SQLType=\"DATE\" ISNULL=\"true\"/>");
  }

  @Test
  public void testTime() throws Exception {
    assumeFalse("TIME type not supported", is(ORACLE));
    executeUpdate("create table data(COLNAME time)");
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
    assumeFalse("TIME type not supported", is(ORACLE));
    testGenerateXml("COLNAME", Types.TIME, null,
        "<COLNAME SQLType=\"TIME\" ISNULL=\"true\"/>");
  }

  @Test
  public void testTimestamp() throws Exception {
    executeUpdate("create table data(COLNAME timestamp)");
    executeUpdate(
        "insert into data(colname) values ({ts '2004-10-06 09:15:30'})");
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

    executeUpdate("create table data(thread int, COLNAME timestamp(3))");

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
                  "select COLNAME from data where thread = " + tt);
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
    testGenerateXml("COLNAME", Types.TIMESTAMP, null,
        "<COLNAME SQLType=\"TIMESTAMP\" ISNULL=\"true\"/>");
  }

  @Test
  public void testClob() throws Exception {
    assumeFalse("CLOB type not supported", is(MYSQL) || is(SQLSERVER));
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
    testGenerateXml("COLNAME", Types.CLOB, clobData,
        "<COLNAME SQLType=\"CLOB\">" + clobData + "</COLNAME>");
  }

  @Test
  public void testClob_null() throws Exception {
    assumeFalse("CLOB type not supported", is(MYSQL) || is(SQLSERVER));
    testGenerateXml("COLNAME", Types.CLOB, null,
        "<COLNAME SQLType=\"CLOB\" ISNULL=\"true\"/>");
  }

  @Test
  public void testNclob() throws Exception {
    assumeTrue("NCLOB type not supported", is(ORACLE));
    String nclobData = "hello world";
    testGenerateXml("COLNAME", Types.NCLOB, nclobData,
        "<COLNAME SQLType=\"NCLOB\">" + nclobData + "</COLNAME>");
  }

  @Test
  public void testNclob_null() throws Exception {
    assumeTrue("NCLOB type not supported", is(ORACLE));
    testGenerateXml("COLNAME", Types.NCLOB, null,
        "<COLNAME SQLType=\"NCLOB\" ISNULL=\"true\"/>");
  }

  @Test
  public void testBlob() throws Exception {
    assumeFalse("BLOB type not supported", is(MYSQL) || is(SQLSERVER));
    byte[] blobData = new byte[12345];
    new Random().nextBytes(blobData);
    String base64BlobData = DatatypeConverter.printBase64Binary(blobData);
    testGenerateXml("COLNAME", Types.BLOB, blobData,
        "<COLNAME SQLType=\"BLOB\" encoding=\"base64binary\">"
        + base64BlobData
        + "</COLNAME>");
  }

  @Test
  public void testBlob_empty() throws Exception {
    assumeFalse("BLOB type not supported", is(MYSQL) || is(SQLSERVER));
    testGenerateXml("COLNAME", Types.BLOB, "",
        "<COLNAME SQLType=\"BLOB\" "
        + (is(ORACLE) ? "ISNULL=\"true\"" : "encoding=\"base64binary\"")
        + "/>");
  }

  @Test
  public void testBlob_null() throws Exception {
    assumeFalse("BLOB type not supported", is(MYSQL) || is(SQLSERVER));
    testGenerateXml("COLNAME", Types.BLOB, null,
        "<COLNAME SQLType=\"BLOB\" ISNULL=\"true\"/>");
  }

  @Test
  public void testSqlxml() throws Exception {
    assumeTrue("SQLXML type not supported", is(ORACLE) || is(SQLSERVER));
    String xmlData = "<motd>hello world</motd>\n";
    executeUpdate("create table data(COLNAME xml)");
    String sql = "insert into data(colname) values (?)";
    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      SQLXML sqlxml = conn.createSQLXML();
      sqlxml.setString(xmlData);
      ps.setSQLXML(1, sqlxml);
      assertEquals(1, ps.executeUpdate());
    }

    String xmlOutput = xmlData.replace("<", "&lt;").replace(">", "&gt;");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\""
        + JdbcFixture.SQLXML
        + "\">"
        + (is(SQLSERVER) ? xmlOutput.trim() : xmlOutput)
        + "</COLNAME>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testSqlxml_empty() throws Exception {
    // Oracle does not support empty XMLTYPE values.
    assumeTrue("SQLXML type not supported", is(SQLSERVER));
    executeUpdate("create table data(COLNAME xml)");
    String sql = "insert into data(colname) values (?)";
    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      SQLXML sqlxml = conn.createSQLXML();
      sqlxml.setString("");
      ps.setSQLXML(1, sqlxml);
      assertEquals(1, ps.executeUpdate());
    }

    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<COLNAME SQLType=\""
        + JdbcFixture.SQLXML
        + "\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testSqlxml_null() throws Exception {
    assumeTrue("SQLXML type not supported", is(ORACLE) || is(SQLSERVER));
    testGenerateXml("COLNAME", Types.SQLXML, "xml", null,
        "<COLNAME SQLType=\"" + JdbcFixture.SQLXML + "\" ISNULL=\"true\"/>");
  }

  @Test
  public void testArray() throws Exception {
    assumeTrue("ARRAY type not supported", is(H2));
    String[] array = { "hello", "world" };
    executeUpdate("create table data(colname array)");
    String sql = "insert into data(colname) values (?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setObject(1, array);
    assertEquals(1, ps.executeUpdate());

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
    assumeTrue("ARRAY type not supported", is(H2));
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
    assumeTrue("OTHER type not supported", is(H2));
    // H2's OTHER type requires a serialized Java object.
    String serializable = "hello world";
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(serializable);
    }

    testGenerateXml("COLNAME", Types.OTHER, buffer.toByteArray(),
        "<COLNAME SQLType=\"OTHER\">" + serializable + "</COLNAME>");
  }

  @Test
  public void testOther_invalidXmlChars() throws Exception {
    assumeTrue("OTHER type not supported", is(H2));
    // H2's OTHER type requires a serialized Java object.
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
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(input.toString());
    }

    testGenerateXml("COLNAME", Types.OTHER, buffer.toByteArray(),
        "<COLNAME SQLType=\"OTHER\">" + output + "</COLNAME>");
  }

  @Test
  public void testOther_null() throws Exception {
    assumeTrue("OTHER type not supported", is(H2));
    testGenerateXml("OTHER", Types.OTHER, null,
        "<OTHER SQLType=\"OTHER\" ISNULL=\"true\"/>");
  }

  @Test
  public void testMultipleTypes() throws Exception {
    executeUpdate(
        "create table data(ID integer, NAME varchar(20), MODIFIED timestamp)");
    executeUpdate(
        "insert into data(id, name, modified) values(1, 'file.txt', null)");
    final String golden = ""
        + "<database>"
        + "<table>"
        + "<table_rec>"
        + "<ID SQLType=\""
        + JdbcFixture.INTEGER
        + "\">1</ID>"
        + "<NAME SQLType=\"VARCHAR\">file.txt</NAME>"
        + "<MODIFIED SQLType=\"TIMESTAMP\" ISNULL=\"true\"/>"
        + "</table_rec>"
        + "</table>"
        + "</database>";
    ResultSet rs = executeQueryAndNext("select * from data");
    String result = generateXml(rs);
    assertEquals(golden, result);
  }

  @Test
  public void testInvalidXmlChars() throws Exception {
    assumeTrue("Only test XML encoding on H2", is(H2));
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

    testGenerateXml("COLNAME", Types.VARCHAR, input.toString(),
        "<COLNAME SQLType=\"VARCHAR\">" + output + "</COLNAME>");
  }

  /** Test all Unicode BMP characters, plus some surrogate pairs. */
  @Test
  public void testValidXmlChars() throws Exception {
    assumeTrue("Only test XML encoding on H2", is(H2));
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

    testGenerateXml("COLNAME", Types.VARCHAR, content,
        "<COLNAME SQLType=\"VARCHAR\">"
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
        + "</COLNAME>");
  }
}
