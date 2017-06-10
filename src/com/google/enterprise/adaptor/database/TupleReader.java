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

import com.google.common.io.BaseEncoding;
import com.google.enterprise.adaptor.IOHelper;

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
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

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

  /** Map from SQL types to SQL type names. */
  private static final HashMap<Integer, String> sqlTypeNames = new HashMap<>();

  static {
    for (Field field : Types.class.getFields()) {
      if (field.getType() == int.class
          && (field.getModifiers() & Modifier.STATIC) == Modifier.STATIC) {
        try {
          sqlTypeNames.put(field.getInt(null), field.getName());
        } catch (IllegalAccessException | IllegalArgumentException e) {
          // These should not be possible with the checks above.
          throw new AssertionError(e);
        }
      }
    }
  }

  private static final String NS = ""; // namespace
  private static final String ROOT_ELEMENT = "database";
  private static final String TABLE_ELEMENT = "table";
  private static final String RECORD_ELEMENT = "table_rec";

  private final ResultSet resultSet;

  public TupleReader(ResultSet resultSet) {
    if (resultSet == null) {
      throw new NullPointerException();
    }
    this.resultSet = resultSet;
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
      handler.startElement(NS, ROOT_ELEMENT, ROOT_ELEMENT, atts);
      handler.startElement(NS, TABLE_ELEMENT, TABLE_ELEMENT, atts);
      emitRow(resultSet, handler);
      handler.endElement(NS, TABLE_ELEMENT, TABLE_ELEMENT);
      handler.endElement(NS, ROOT_ELEMENT, ROOT_ELEMENT);
      handler.endDocument();
    } catch (SQLException ex) {
      throw new SAXException(ex);
    }
  }

  protected void emitRow(ResultSet resultSet, ContentHandler handler)
      throws SQLException, SAXException, IOException {
    AttributesImpl atts = new AttributesImpl();
    handler.startElement(NS, RECORD_ELEMENT, RECORD_ELEMENT, atts);
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int colCount = rsmd.getColumnCount();

    for (int col = 1; col <= colCount; col++) {
      atts.clear();
      String columnName = rsmd.getColumnLabel(col);
      int sqlType = rsmd.getColumnType(col);
      String sqlTypeName = getColumnTypeName(sqlType, rsmd, col);
      atts.addAttribute("", "SQLType", "SQLType", "NMTOKEN", sqlTypeName);
      log.fine("sqlTypeName: " + sqlTypeName);
      switch (sqlType) {
        case Types.DATE:
          Date dateValue = resultSet.getDate(col);
          if (dateValue != null) {
            try (Writer outWriter = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              outWriter.write(DATEFMT.format(dateValue));
            }
          }
          break;
        case Types.TIMESTAMP:
          // TODO: probably need to change to calling getTimestamp with Calendar
          // parameter.
          Timestamp tsValue = resultSet.getTimestamp(col);
          if (tsValue != null) {
            try (Writer outWriter = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              String timezone = TIMEZONEFMT.format(tsValue);
              // java timezone not ISO compliant
              timezone =
                  timezone.substring(0, timezone.length() - 2) + ":"
                      + timezone.substring(timezone.length() - 2);
              outWriter.write(DATEFMT.format(tsValue) + "T"
                  + TIMEFMT.format(tsValue) + timezone);
            }
          }
          break;
        case Types.TIME:
          Time timeValue = resultSet.getTime(col);
          if (timeValue != null) {
            try (Writer outWriter = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              String timezone = TIMEZONEFMT.format(timeValue);
              // java timezone not ISO compliant
              timezone =
                  timezone.substring(0, timezone.length() - 2) + ":"
                      + timezone.substring(timezone.length() - 2);
              outWriter.write(TIMEFMT.format(timeValue) + timezone);
            }
          }
          break;
        case Types.BLOB:
          Blob blob = resultSet.getBlob(col);
          if (blob != null) {
            try (InputStream lob = resultSet.getBinaryStream(col)) {
              atts.addAttribute("", "encoding", "encoding", "NMTOKEN",
                  "base64binary");
              handler.startElement("", columnName, columnName, atts);
              copyBase64EncodedData(lob, handler);
            } finally {
              try {
                blob.free();
              } catch (Exception e) {
                log.log(Level.FINEST, "Error closing BLOB", e);
              }
            }
          }
          break;
        case Types.CLOB:
          Clob clob = resultSet.getClob(col);
          if (clob != null) {
            try (Reader reader = clob.getCharacterStream()) {
              handler.startElement("", columnName, columnName, atts);
              copyValidXmlCharacters(reader, handler);
            } finally {
              try {
                clob.free();
              } catch (Exception e) {
                log.log(Level.FINEST, "Error closing CLOB", e);
              }
            }
          }
          break;
        case Types.NCLOB:
          NClob nclob = resultSet.getNClob(col);
          if (nclob != null) {
            try (Reader reader = resultSet.getNCharacterStream(col)) {
              handler.startElement("", columnName, columnName, atts);
              copyValidXmlCharacters(reader, handler);
            } finally {
              try {
                nclob.free();
              } catch (Exception e) {
                log.log(Level.FINEST, "Error closing NCLOB", e);
              }
            }
          }
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          try (InputStream lob = resultSet.getBinaryStream(col)) {
            if (lob != null) {
              atts.addAttribute("", "encoding", "encoding", "NMTOKEN",
                  "base64binary");
              handler.startElement("", columnName, columnName, atts);
              copyBase64EncodedData(lob, handler);
            }
          }
          break;
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.FLOAT:
        case Types.DECIMAL:
        case Types.NUMERIC:
        case Types.DOUBLE:
        case Types.INTEGER:
          String string = resultSet.getString(col);
          if (string != null) {
            try (Writer outWriter = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              outWriter.write(string);
            }
          }
          break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
          string = resultSet.getString(col);
          if (string != null) {
            handler.startElement("", columnName, columnName, atts);
            copyValidXmlCharacters(new StringReader(string), handler);
          }
          break;
        case Types.LONGVARCHAR:
          try (Reader reader = resultSet.getCharacterStream(col)) {
            if (reader != null) {
              handler.startElement("", columnName, columnName, atts);
              copyValidXmlCharacters(reader, handler);
            }
          }
          break;
        case Types.LONGNVARCHAR:
          try (Reader nReader = resultSet.getNCharacterStream(col)) {
            if (nReader != null) {
              handler.startElement("", columnName, columnName, atts);
              copyValidXmlCharacters(nReader, handler);
            }
          }
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          Object value = resultSet.getObject(col);
          if (value != null) {
            try (Writer outWriter = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              outWriter.write(String.valueOf(value));
            }
          }
          break;
        case Types.OTHER:
        default:
          value = resultSet.getObject(col);
          if (value != null) {
            try (Writer writer = new ContentHandlerWriter(handler)) {
              handler.startElement("", columnName, columnName, atts);
              writer.write(String.valueOf(value));
            }
          }
          break;
      }
      if (resultSet.wasNull()) {
        atts.addAttribute("", "ISNULL", "ISNULL", "NMTOKEN", "true");
        handler.startElement("", columnName, columnName, atts);
      }
      handler.endElement("", columnName, columnName);
    }
    handler.endElement(NS, RECORD_ELEMENT, RECORD_ELEMENT);
  }

  /**
   * Return a SQL type name for the column type.
   * Use this instead of {@link ResultSetMetaData.getColumnTypeName()} 
   * because drivers return different names.
   *
   * @param sqlType a type from java.sql.Types
   * @return a name for the type from the first non-null value found
   *     looking in java.sql.Types, calling ResultSetMetaData.getColumnTypeName,
   *      and falling back to a string of the raw integer type itself
   */
  private static String getColumnTypeName(int sqlType, ResultSetMetaData rsmd,
      int columnIndex) throws SQLException {
     String sqlTypeName = sqlTypeNames.get(sqlType);
     if (sqlTypeName == null) {
       // Try the database's name for non-standard types.
       sqlTypeName = rsmd.getColumnTypeName(columnIndex);
       if (sqlTypeName == null || sqlTypeName.isEmpty()) {
         sqlTypeName = String.valueOf(sqlType);
       }
     }
     return sqlTypeName;
  }

  /**
   * Copies valid XML unicode characters as defined in the XML 1.0 standard -
   * http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char
   * Invalid characters are replaced by U+FFFD REPLACEMENT CHARACTER.
   *
   * Note: The XML standard refers to Unicode characters, but Java
   * uses UTF-16 characters, including surrogates to represent
   * characters U+10000 and beyond.
   */
  private static void copyValidXmlCharacters(Reader in, ContentHandler handler)
      throws IOException, SAXException {
    char[] buffer = new char[8192];
    int len;
    while ((len = in.read(buffer)) != -1) {
      for (int i = 0; i < len; i++) {
        char current = buffer[i];
        if (current != '\t' && current != '\n' && current != '\r'
            && (current < '\u0020' || current > '\uFFFD')) {
          buffer[i] = '\uFFFD'; // Unicode REPLACEMENT CHARACTER
        }
      }
      handler.characters(buffer, 0, len);
    }
  }

  /**
   * Writes a Base64-encoded copy of an InputStream to a Writer.
   *
   * @param in the input data stream
   * @param out the output writer
   */
  private static void copyBase64EncodedData(InputStream in,
      ContentHandler handler) throws IOException {
    Writer writer = new BufferedWriter(new ContentHandlerWriter(handler));
    IOHelper.copyStream(in, BaseEncoding.base64().encodingStream(writer));
  }

  /**
   * A writer that writes to the {@link org.xml.sax.ContentHandler}'s 
   * character data area.
   */
  private static class ContentHandlerWriter extends Writer {
    private final ContentHandler handler;

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
