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

import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

/**
 * A XMLReader that reads one row of a ResultSet and outputs XML events
 * according to database XML. An example of database XML is below.
 * <p>
 * <pre> 
 * &lt;?xml version="1.0" encoding="UTF-8"?&gt;
 * &lt;database&gt;
 * &lt;table&gt;
 * &lt;table_rec&gt;
 * &lt;_url SQLType="CHAR"&gt;
 *  http://www.corp.google.com/search/roomsearch.php?q=Albuquerque&lt;/_url&gt;
 * &lt;room SQLType="CHAR">Albuquerque&lt;/room&gt;
 * &lt;location SQLType="CHAR">Mountain View&lt;/location&gt;
 * &lt;building SQLType="CHAR">41&lt;/building&gt;
 * &lt;floor SQLType="CHAR">1&lt;/floor&gt;
 * &lt;phone SQLType="CHAR">650-623-  6904&lt;/phone&gt;
 * &lt;capacity SQLType="CHAR">10-12 people&lt;/capacity&gt;
 * &lt;equipment SQLType="CHAR">Mitsubishi projector&lt;/equipment&gt;
 * &lt;notes SQLType="CHAR"/&gt;
 * &lt;map SQLType="CHAR"&gt;
 *  http://www/facilities/images/Bldg41ConfRoomsFloor1.gif&lt;/map&gt;
 * &lt;_mimetype SQLType="CHAR">text/html&lt;/_mimetype&gt;
 * &lt;/table_rec&gt;
 * &lt;/table&gt;
 * &lt;/database&gt;
 * </pre>
 */
class TupleReader extends XMLFilterImpl implements XMLReader {
  private static final Logger log = Logger.getLogger(TupleReader.class
      .getName());

  // format data, time, timestamp according to ISO 8601
  private static final DateFormat DATEFMT = new SimpleDateFormat("yyyy-MM-dd");
  private static final DateFormat TIMEFMT = new SimpleDateFormat("hh:mm:ss");
  private static final DateFormat TIMEZONEFMT = new SimpleDateFormat("Z");

  private final ResultSet resultSet;
  private final String ns; // namespace
  private final String rootElement;
  private final String tableElement;
  private final String recordElement;

  public TupleReader(ResultSet resultSet) {
    this(resultSet, "", "database", "table", "table_rec");
  }

  public TupleReader(ResultSet resultSet, String ns, String rootElement,
      String tableElement, String recordElement) {
    if (resultSet == null) {
      throw new NullPointerException();
    }
    this.resultSet = resultSet;
    this.ns = ns;
    this.rootElement = rootElement;
    this.tableElement = tableElement;
    this.recordElement = recordElement;
  }

  /**
   * Parse the row and generate SAX events.
   * 
   * @param ignoredInputSource not used
   */
  @Override
  public void parse(InputSource ignoredInputSource) throws IOException,
      SAXException {
    try {
      ContentHandler handler = super.getContentHandler();
      AttributesImpl atts = new AttributesImpl();
      handler.startDocument();
      if (rootElement != null) {
        handler.startElement(ns, rootElement, rootElement, atts);
      }
      if (tableElement != null) {
        handler.startElement(ns, tableElement, tableElement, atts);
      }
      emitRow(resultSet, handler);
      if (tableElement != null) {
        handler.endElement(ns, tableElement, tableElement);
      }
      if (rootElement != null) {
        handler.endElement(ns, rootElement, rootElement);
      }
      handler.endDocument();
    } catch (SQLException ex) {
      throw new SAXException(ex);
    }
  }

  protected void emitRow(ResultSet resultSet, ContentHandler handler)
      throws SQLException, SAXException, IOException {
    AttributesImpl atts = new AttributesImpl();
    if (recordElement != null) {
      handler.startElement(ns, recordElement, recordElement, atts);
    }
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int colCount = rsmd.getColumnCount();

    Writer outWriter = new BufferedWriter(new ContentHandlerWriter(handler));
    for (int col = 1; col <= colCount; col++) {
      atts.clear();
      String columnName = rsmd.getColumnName(col);
      int sqlType = rsmd.getColumnType(col);
      String sqlTypeName = getColumnTypeName(sqlType);
      if (sqlTypeName != null) {
        atts.addAttribute("", "SQLType", "SQLType", "NMTOKEN", sqlTypeName);
      }
      log.fine("sqlTypeName: " + sqlTypeName);
      if (resultSet.getObject(col) == null) {
        atts.addAttribute("", "ISNULL", "ISNULL", "NMTOKEN", "true");
        handler.startElement("", columnName, columnName, atts);
        // TODO: Re-examine this need for flush.
        outWriter.flush();
        handler.endElement("", columnName, columnName);
        continue;
      }
      switch (sqlType) {
        case Types.DATE:
          Date dateValue = resultSet.getDate(col);
          handler.startElement("", columnName, columnName, atts);
          outWriter.write(DATEFMT.format(dateValue));
          break;
        case Types.TIMESTAMP:
          // TODO: probably need to change to calling getTimestamp with Calendar
          // parameter.
          Timestamp tsValue = resultSet.getTimestamp(col);
          handler.startElement("", columnName, columnName, atts);
          String timezone = TIMEZONEFMT.format(tsValue);
          // java timezone not ISO compliant
          timezone =
              timezone.substring(0, timezone.length() - 2) + ":"
                  + timezone.substring(timezone.length() - 2);
          outWriter.write(DATEFMT.format(tsValue) + "T"
              + TIMEFMT.format(tsValue) + timezone);
          break;
        case Types.TIME:
          Time timeValue = resultSet.getTime(col);
          handler.startElement("", columnName, columnName, atts);
          timezone = TIMEZONEFMT.format(timeValue);
          // java timezone not ISO compliant
          timezone =
              timezone.substring(0, timezone.length() - 2) + ":"
                  + timezone.substring(timezone.length() - 2);
          outWriter.write(TIMEFMT.format(timeValue) + timezone);
          break;
        case Types.BLOB:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          InputStream lob = resultSet.getBinaryStream(col);
          // so encode using Base64
          atts.addAttribute("", "encoding", "encoding", "NMTOKEN",
              "base64binary");
          handler.startElement("", columnName, columnName, atts);
          encode(lob, outWriter);
          break;
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.FLOAT:
        case Types.DECIMAL:
        case Types.NUMERIC:
        case Types.DOUBLE:
        case Types.INTEGER:
          String string = resultSet.getString(col);
          handler.startElement("", columnName, columnName, atts);
          outWriter.write(string);
          break;
        case Types.CHAR:
        case Types.CLOB:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          Reader reader = resultSet.getCharacterStream(col);
          handler.startElement("", columnName, columnName, atts);
          copyValidXMLCharacters(reader, outWriter);
          reader.close();
          break;
        case Types.NCHAR:
        case Types.NCLOB:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
          Reader nReader = resultSet.getNCharacterStream(col);
          handler.startElement("", columnName, columnName, atts);
          copyValidXMLCharacters(nReader, outWriter);
          nReader.close();
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          Boolean value = resultSet.getBoolean(col);
          handler.startElement("", columnName, columnName, atts);
          outWriter.write(String.valueOf(value));
          break;
        case Types.OTHER:
        default:
          string = "" + resultSet.getObject(col);
          handler.startElement("", columnName, columnName, atts);
          outWriter.write(string);
          break;
      }
      outWriter.flush();
      handler.endElement("", columnName, columnName);
    }
    outWriter.close();
    if (recordElement != null) {
      handler.endElement(ns, recordElement, recordElement);
    }
  }

  /**
   * Return a SQL type name for the column type.
   * Use this instead of {@link ResultSetMetaData.getColumnTypeName()} 
   * because drivers return different names. MySQL returns very stupid
   * names (TEXT => "BLOB").
   *
   * @param sqlType a type from java.sql.Types
   * @return a string name, null if sqlType is unknown
   */
  private static String getColumnTypeName(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
        return "BIT";
      case Types.TINYINT:
        return "TINYINT";
      case Types.SMALLINT:
        return "SMALLINT";
      case Types.INTEGER:
        return "INTEGER";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.REAL:
        return "REAL";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.NUMERIC:
        return "NUMERIC";
      case Types.DECIMAL:
        return "DECIMAL";
      case Types.CHAR:
        return "CHAR";
      case Types.VARCHAR:
        return "VARCHAR";
      case Types.LONGVARCHAR:
        return "LONGVARCHAR";
      case Types.DATE:
        return "DATE";
      case Types.TIME:
        return "TIME";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      case Types.BINARY:
        return "BINARY";
      case Types.VARBINARY:
        return "VARBINARY";
      case Types.LONGVARBINARY:
        return "LONGVARBINARY";
      case Types.NULL:
        return "NULL";
      case Types.OTHER:
        return "OTHER";
      case Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
      case Types.DISTINCT:
        return "DISTINCT";
      case Types.STRUCT:
        return "STRUCT";
      case Types.ARRAY:
        return "ARRAY";
      case Types.BLOB:
        return "BLOB";
      case Types.CLOB:
        return "CLOB";
      case Types.REF:
        return "REF";
      case Types.DATALINK:
        return "DATALINK";
      case Types.BOOLEAN:
        return "BOOLEAN";
      case Types.NCHAR:
        return "NCHAR";
      case Types.NVARCHAR:
        return "NVARCHAR";
      case Types.NCLOB:
        return "NCLOB";
      case Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
      default:
        return String.valueOf(sqlType);
    }
  }

  /**
   * Copies valid XML unicode characters as defined in the XML 1.0 standard -
   * http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char.
   * Note: Writer should be a BufferedWriter since we write one byte at a time.
   * 
   * @return number of characters copied.
   */
  private static int copyValidXMLCharacters(Reader in, Writer out)
      throws IOException {
    char[] buffer = new char[1024];
    int len = in.read(buffer);
    int total = 0;
    int i = 0;
    char current = 0;
    while (len != -1) {
      for (i = 0; i < len; i++) {
        current = buffer[i];
        if ((current == 0x9) || (current == 0xA) || (current == 0xD)
            || ((current >= 0x20) && (current <= 0xD7FF))
            || ((current >= 0xE000) && (current <= 0xFFFD))
            || ((current >= 0x10000) && (current <= 0x10FFFF))) {
          out.write(current);
          total += 1;
        }
      }
      len = in.read(buffer);
    }
    return total;
  }
  
  private static int encode(InputStream inStream, Writer out, int bufferSize)
      throws IOException {
    int len = 0;
    byte[] buffer = new byte[bufferSize];
    int numOfBytes = inStream.read(buffer, 0, buffer.length);
    byte[] partToEncode = Arrays.copyOf(buffer, numOfBytes);
    String converted = DatatypeConverter.printBase64Binary(partToEncode);
    while (numOfBytes != -1) {
      out.write(converted);
      len += numOfBytes;
      numOfBytes = inStream.read(buffer, 0, buffer.length);
    }
    return len;
  }

  /**
   * Write out a whole {@link InputStream} using Base64 encoding,
   * using default buffer size of 1024*3.
   *
   * WARNING: This method will not flush the Writer 'out' (second argument). 
   * You'll have to do this yourself, or loose some of your precious data. 
   *
   * @param inStream the input data stream
   * @param out output writer
   * @return number of bytes processed
   */
  private static int encode(InputStream inStream, Writer out)
      throws IOException {
    return encode(inStream, out, 1024 * 3);
  }

  /**
   * A writer that writes to the {@link org.xml.sax.ContentHandler}'s 
   * character data area.
   */
  private static class ContentHandlerWriter extends Writer {
    private ContentHandler handler;

    /**
     * Create a writer to a handler.
     *
     * @param handler a {@link org.xml.sax.ContentHandler} object, not null
     */
    public ContentHandlerWriter(ContentHandler handler) {
      this.handler = handler;
    }

    /**
     * Do nothing (no-op).
     */
    public void close() throws IOException {
      // no-op
    }

    /**
     * Do nothing (no-op).
     */
    public void flush() throws IOException {
      // no-op
    }

    /**
     * Write a character buffer to the character data of the handler.
     * <p>
     * Calls the handler's characters method.
     *
     * @param cbuf the character buffer
     * @param off the starting offset in cbuf
     * @param len length of data in cbuf
     * @see org.xml.sax.ContentHandler#characters()
     */
    public void write(char[] cbuf, int off, int len) throws IOException {
      try {
        handler.characters(cbuf, off, len);
      } catch (SAXException ex) {
        throw new IOException(ex);
      }
    }
  }
}
