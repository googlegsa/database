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

import static com.google.common.base.Charsets.UTF_8;

import com.google.enterprise.adaptor.IOHelper;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;

import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/** Generate a response according to a SQL result */
public abstract class ResponseGenerator {
  private static final Logger log
      = Logger.getLogger(ResponseGenerator.class.getName());

  private final Map<String, String> cfg;
  private final String displayUrlCol; // can be null

  protected Map<String, String> getConfig() {
    return cfg;
  }

  protected ResponseGenerator(Map<String, String> config) {
    if (null == config) {
      throw new NullPointerException();
    }
    this.cfg = Collections.unmodifiableMap(config);
    log.config("entire config map=" + cfg);
    displayUrlCol = getConfig().get("displayUrlCol");
    log.config("displayUrlCol=" + displayUrlCol);
  }

  /**
   * This method will generate the Response according to the data returned by
   * SQL statements.
   *
   * @param rs the content for this Response will be fetched out of this object
   * @param res the Response to fill into
   * @return true if display url gets set
   * @throws SQLException when call to getString() goes awry
   */
  protected boolean overrideDisplayUrl(ResultSet rs, Response res)
      throws SQLException {
    if (null != displayUrlCol) {
      String dispUrl = rs.getString(displayUrlCol);
      if (null == dispUrl) {
        log.log(Level.FINE, "display url at col {0} is null",
            displayUrlCol);
      } else {
        try {
          URI dispUri = new URI(dispUrl);
          res.setDisplayUrl(dispUri);
          log.log(Level.FINE, "overrode display url: {0}", dispUrl);
          return true;
        } catch (URISyntaxException uriException) {
          log.log(Level.WARNING, "override display url invalid: {0} {1}",
              new Object[] {dispUrl, uriException});
        }
      }
    }
    log.finest("not overriding display url");
    return false;
  }

  /**
   * This method will generate the Response according to the data returned by
   * SQL statements.
   *
   * @param rs the content for this Response will be fetched out of this object
   * @param resp the Response to fill into
   * @throws IOException when things go awry
   * @throws SQLException when things go awry
   */
  public abstract void generateResponse(ResultSet rs, Response resp)
      throws IOException, SQLException;

  @Override
  public String toString() {
    return getClass().getName() + "(" + cfg + ")";
  }

  public static ResponseGenerator rowToText(Map<String, String> config) {
    return new RowToText(config);
  }

  public static ResponseGenerator urlColumn(Map<String, String> config) {
    return new UrlColumn(config);
  }

  public static ResponseGenerator urlAndMetadataLister(
      Map<String, String> config) {
    return new UrlAndMetadataLister(config);
  }

  public static ResponseGenerator filepathColumn(Map<String, String> config) {
    return new FilepathColumn(config);
  }

  @Deprecated
  /** This method has been deprecated, Use {@link #contentColumn contentColumn}
   *  instead.
   **/
  public static ResponseGenerator blobColumn(Map<String, String> config) {
    return contentColumn(config);
  }

  public static ResponseGenerator contentColumn(Map<String, String> config) {
    return new ContentColumn(config);
  }

  public static ResponseGenerator rowToHtml(Map<String, String> config)
      throws TransformerConfigurationException, IOException {
    return new RowToHtml(config);
  }

  private static class RowToHtml extends ResponseGenerator {
    private static final String CONTENT_TYPE = "text/html; charset=utf-8";
    private static final String DEFAULT_STYLESHEET = "resources/dbdefault.xsl";

    private final Templates template;

    RowToHtml(Map<String, String> config)
        throws TransformerConfigurationException, IOException {
      super(config);
      String stylesheetFilename = getConfig().get("stylesheet");
      InputStream xsl = null;
      if (null != stylesheetFilename) {
        xsl = new FileInputStream(stylesheetFilename);
      } else {
        String stylesheetName = DEFAULT_STYLESHEET;
        xsl = this.getClass().getResourceAsStream(stylesheetName);
        if (xsl == null) {
          throw new AssertionError("Default stylesheet not found in resources");
        }
      }

      TransformerFactory transFactory = TransformerFactory.newInstance();
      template = transFactory.newTemplates(new StreamSource(xsl));
      xsl.close();
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      resp.setContentType(CONTENT_TYPE);
      overrideDisplayUrl(rs, resp);
      TupleReader reader = new TupleReader(rs);
      Source source = new SAXSource(reader, /*ignored*/new InputSource());
      Result des = new StreamResult(resp.getOutputStream());
      try {
        Transformer trans = template.newTransformer();
        // output is html, so we don't need xml declaration
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.transform(source, des);
      } catch (TransformerException e) {
        throw new RuntimeException("Error in applying xml stylesheet", e);
      }
    }
  }

  private static class RowToText extends ResponseGenerator {
    private static final String CONTENT_TYPE = "text/plain; charset=utf-8";
    private static final Charset ENCODING = Charset.forName("UTF-8");

    RowToText(Map<String, String> config) {
      super(config);
      // no rowToText mode specific configuration
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      ResultSetMetaData rsMetaData = rs.getMetaData();
      int numberOfColumns = rsMetaData.getColumnCount();

      // If we have data then create lines of resulting document.
      StringBuilder line1 = new StringBuilder();
      StringBuilder line2 = new StringBuilder();
      StringBuilder line3 = new StringBuilder();
      for (int i = 1; i < (numberOfColumns + 1); i++) {
        String tableName = rsMetaData.getTableName(i);
        String columnName = rsMetaData.getColumnLabel(i);
        String value = rs.getString(i);
        if (null == value) {
          value = "" + rs.getObject(i);
        }
        line1.append(",");
        line1.append(makeIntoCsvField(tableName));
        line2.append(",");
        line2.append(makeIntoCsvField(columnName));
        line3.append(",");
        line3.append(makeIntoCsvField(value));
      }
      String document = line1.substring(1) + "\n" + line2.substring(1) + "\n"
          + line3.substring(1) + "\n";

      resp.setContentType(CONTENT_TYPE);
      overrideDisplayUrl(rs, resp);
      resp.getOutputStream().write(document.getBytes(ENCODING));
    }

    private static String makeIntoCsvField(String s) {
      if (null == s) {
        throw new NullPointerException();
      }
      /*
       * Fields that contain a special character (comma, newline,
       * or double quote), must be enclosed in double quotes.
       * <...> If a field's value contains a double quote character
       * it is escaped by placing another double quote character next to it.
       */
      String doubleQuote = "\"";
      boolean containsSpecialChar = s.contains(",")
          || s.contains("\n") || s.contains(doubleQuote);
      if (containsSpecialChar) {
        s = s.replace(doubleQuote, doubleQuote + doubleQuote);
        s = doubleQuote + s + doubleQuote;
      }
      return s;
    }
  }

  private abstract static class SingleColumnContent extends ResponseGenerator {
    private final String col;
    private final String contentTypeOverride; // can be null
    private final String contentTypeCol; // can be null

    String getContentColumnName() {
      return col;
    }

    SingleColumnContent(Map<String, String> config) {
      super(config);
      col = getConfig().get("columnName"); 
      if (null == col) {
        throw new NullPointerException("columnName needs to be provided");
      }
      log.config("col=" + col);
      contentTypeOverride = getConfig().get("contentTypeOverride");
      contentTypeCol = getConfig().get("contentTypeCol");
      log.config("contentTypeOverride=" + contentTypeOverride);
      log.config("contentTypeCol=" + contentTypeCol);
      if (null != contentTypeOverride && null != contentTypeCol) {
        throw new InvalidConfigurationException("cannot provide both "
            + "contentTypeOverride and contentTypeCol");
      }
    }

    boolean overrideContentType(ResultSet rs, Response res) 
        throws SQLException {
      if (null != contentTypeOverride) {
        res.setContentType(contentTypeOverride);
        log.log(Level.FINE, "overrode content type: {0}", contentTypeOverride);
        return true;
      } else if (null != contentTypeCol) {
        String ct = rs.getString(contentTypeCol);
        if (null == ct) {
          log.log(Level.FINE, "content type at col {0} is null",
              contentTypeCol);
        } else {
          res.setContentType(ct);
          log.log(Level.FINE, "overrode content type: {0}", ct);
          return true;
        }
      }
      log.fine("not overriding content type");
      return false;
    }
  }

  private static class UrlColumn extends SingleColumnContent {
    UrlColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      String urlStr = rs.getString(getContentColumnName());
      URL url = new URL(urlStr);
      java.net.URLConnection con = url.openConnection();
      if (!overrideContentType(rs, resp)) {
        String contentType = con.getContentType();
        if (null != contentType) {
          resp.setContentType(contentType);
        }
      }
      try {
        if (!overrideDisplayUrl(rs, resp)) {
          resp.setDisplayUrl(url.toURI());
        }
      } catch (URISyntaxException ex) {
        String errmsg = urlStr + " is not a valid URI";
        throw new IllegalStateException(errmsg, ex);
      }
      InputStream in = null;
      try {
        in = con.getInputStream();
        OutputStream out = resp.getOutputStream();
        com.google.enterprise.adaptor.IOHelper.copyStream(in, out);
      } finally {
        if (null != in) {
          in.close();
        }
      }
    }
  }

  private static class UrlAndMetadataLister extends SingleColumnContent {
    UrlAndMetadataLister(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      log.info("calling generateResponse for urlAndMetadataLister!");
      resp.respondNotFound();
      return;
    }
  }

  private static class FilepathColumn extends SingleColumnContent {
    FilepathColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      overrideContentType(rs, resp);
      overrideDisplayUrl(rs, resp);
      String path = rs.getString(getContentColumnName());
      InputStream in = null;
      try {
        in = new FileInputStream(path);
        OutputStream out = resp.getOutputStream();
        com.google.enterprise.adaptor.IOHelper.copyStream(in, out);
      } finally {
        if (null != in) {
          in.close();
        }
      }
    }
  }

  private static class ContentColumn extends SingleColumnContent {
    ContentColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      overrideContentType(rs, resp);
      overrideDisplayUrl(rs, resp);

      ResultSetMetaData rsMetaData = rs.getMetaData();
      int index = rs.findColumn(getContentColumnName());
      int columnType = rsMetaData.getColumnType(index);
      log.log(Level.FINEST, "Content column name: {0}, Type: {1}",
          new Object[] {getContentColumnName(), columnType});

      OutputStream out = resp.getOutputStream();
      switch (columnType) {
        case Types.LONGVARCHAR:
          try (Reader reader = rs.getCharacterStream(index);
              Writer writer = new OutputStreamWriter(out, UTF_8)) {
            if (reader != null) {
              copy(reader, writer);
            }
          }
          break;
        case Types.VARBINARY:
          try (ByteArrayInputStream in =
              new ByteArrayInputStream(rs.getBytes(index))) {
            IOHelper.copyStream(in, out);
          }
          break;
        case Types.LONGVARBINARY:
          try (InputStream in = rs.getBinaryStream(index)) {
            if (in != null) {
              IOHelper.copyStream(in, out);
            }
          }
          break;
        case Types.LONGNVARCHAR:
          try (Reader reader = rs.getNCharacterStream(index);
              Writer writer = new OutputStreamWriter(out, UTF_8)) {
            if (reader != null) {
              copy(reader, writer);
            }
          }
          break;
        case Types.CLOB:
          Clob clob = rs.getClob(index);
          if (clob != null) {
            try (Reader reader = clob.getCharacterStream();
                Writer writer = new OutputStreamWriter(out, UTF_8)) {
              copy(reader, writer);
            } finally {
              try {
                clob.free();
              } catch (java.sql.SQLFeatureNotSupportedException
                  | UnsupportedOperationException unsupported) {
                // let JVM garbage collection deal with it
                log.log(Level.FINEST, "Error closing clob", unsupported);
              }
            }
          }
          break;
        case Types.NCLOB:
          NClob nclob = rs.getNClob(index);
          if (nclob != null) {
            try (Reader reader = nclob.getCharacterStream();
                Writer writer = new OutputStreamWriter(out, UTF_8)) {
              if (reader != null) {
                copy(reader, writer);
              }
            } finally {
              try {
                nclob.free();
              } catch (java.sql.SQLFeatureNotSupportedException
                  | UnsupportedOperationException unsupported) {
                // let JVM garbage collection deal with it
                log.log(Level.FINEST, "Error closing nclob", unsupported);
              }
            }
          }
          break;
        case Types.BLOB:
          Blob blob = rs.getBlob(index);
          if (blob != null) {
            try (InputStream in = blob.getBinaryStream()) {
              IOHelper.copyStream(in, out);
            } finally {
              try {
                blob.free();
              } catch (java.sql.SQLFeatureNotSupportedException
                  | UnsupportedOperationException unsupported) {
                // let JVM garbage collection deal with it
                log.log(Level.FINEST, "Error closing blob", unsupported);
              }
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
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
          String str = rs.getString(index);
          if (str != null) {
            out.write(str.getBytes(UTF_8));
          }
          break;
        default:
          //TODO(srinivas): handle BFILE
          log.log(Level.FINEST, "Column type not handled: {0}", columnType);
          break;
      }
    }

    private void copy(Reader reader, Writer writer) throws IOException {
      char[] buffer = new char[8192];
      int len;
      while ((len = reader.read(buffer)) != -1) {
        writer.write(buffer, 0, len);
      }
      writer.flush();
    }
  }
}
