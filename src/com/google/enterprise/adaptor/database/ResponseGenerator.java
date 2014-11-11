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

import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;

import org.xml.sax.InputSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
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
  protected static final Logger log
      = Logger.getLogger(ResponseGenerator.class.getName());
  
  protected final Map<String, String> cfg;

  protected ResponseGenerator(Map<String, String> config) {
    if (null == config) {
      throw new NullPointerException();
    }
    this.cfg = Collections.unmodifiableMap(config);
  }

  /**
   * This method will generate the Response according to the data returned by
   * SQL statements.
   *
   * @param rs the content for this Response will be fetched out of this object
   * @param resp the Response to fill into
   * @throws IOException
   * @throws SQLException
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

  public static ResponseGenerator filepathColumn(Map<String, String> config) {
    return new FilepathColumn(config);
  }

  public static ResponseGenerator blobColumn(Map<String, String> config) {
    return new BlobColumn(config);
  }

  public static ResponseGenerator rowToHtml(Map<String, String> config)
      throws TransformerConfigurationException, IOException {
    return new RowToHtml(config);
  }

  private static class RowToHtml extends ResponseGenerator {
    private static final String CONTENT_TYPE = "text/html; charset=utf-8";
    private static final String DEFAULT_STYLESHEET = "resources/dbdefault.xsl";

    private final Templates template;

    public RowToHtml(Map<String, String> config)
        throws TransformerConfigurationException, IOException {
      super(config);
      String stylesheetFilename = config.get("stylesheet");
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

    public RowToText(Map<String, String> config) {
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
        String columnName = rsMetaData.getColumnName(i);
        Object value = rs.getObject(i);
        line1.append(",");
        line1.append(makeIntoCsvField(tableName));
        line2.append(",");
        line2.append(makeIntoCsvField(columnName));
        line3.append(",");
        line3.append(makeIntoCsvField("" + value));
      }
      String document = line1.substring(1) + "\n" + line2.substring(1) + "\n"
          + line3.substring(1) + "\n";

      resp.setContentType(CONTENT_TYPE);
      resp.getOutputStream().write(document.getBytes(ENCODING));
    }

    private static String makeIntoCsvField(String s) {
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
    final String col;
    private final String contentTypeOverride; // can be null
    private final String contentTypeCol; // can be null

    SingleColumnContent(Map<String, String> config) {
      super(config);
      col = cfg.get("columnName"); 
      if (null == col) {
        throw new NullPointerException("columnName needs to be provided");
      }
      contentTypeOverride = cfg.get("contentTypeOverride");
      contentTypeCol = cfg.get("contentTypeCol");
      if (null != contentTypeOverride && null != contentTypeCol) {
        throw new InvalidConfigurationException("cannot provide both "
            + "contentTypeOverride and contentTypeCol");
      }
    }

    boolean overrideContentType(ResultSet rs, Response res) 
        throws SQLException {
      if (null != contentTypeOverride) {
        res.setContentType(contentTypeOverride);
        return true;
      } else if (null != contentTypeCol) {
        String ct = rs.getString(contentTypeCol);
        if (null != ct) {
          res.setContentType(ct);
          return true;
        }
      }
      return false;
    }
  }

  private static class UrlColumn extends SingleColumnContent {
    public UrlColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      String urlStr = rs.getString(col);
      URL url = new URL(urlStr);
      java.net.URLConnection con = url.openConnection();
      if (!overrideContentType(rs, resp)) {
        String contentType = con.getContentType();
        if (null != contentType) {
          resp.setContentType(contentType);
        }
      }
      try {
        resp.setDisplayUrl(url.toURI());
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

  private static class FilepathColumn extends SingleColumnContent {
    public FilepathColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      overrideContentType(rs, resp);
      String path = rs.getString(col);
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

  private static class BlobColumn extends SingleColumnContent {
    public BlobColumn(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      overrideContentType(rs, resp);
      Blob blob = rs.getBlob(col);
      InputStream in = blob.getBinaryStream();
      OutputStream out = resp.getOutputStream();
      com.google.enterprise.adaptor.IOHelper.copyStream(in, out);
      in.close();
      if (!(blob instanceof javax.sql.rowset.serial.SerialBlob)) {
        // SerialBlob is adamant about not supporting free
        try {
          blob.free();
        } catch (java.sql.SQLFeatureNotSupportedException | 
            UnsupportedOperationException unsupported) {
          // let JVM garbage collection deal with it
        }
      }
    }
  }
}
