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

import com.google.enterprise.adaptor.Response;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

/** Generate a response according to a SQL result */
public abstract class ResponseGenerator {
  /**
   * This method will generate the Response according to the data returned by
   * SQL statements.
   *
   * @param rs, the content for this Response will be fetched out of this object
   * @param resp, the Response to fill into
   * @throws IOException
   * @throws SQLException
   */
  public abstract void generateResponse(ResultSet rs, Response resp)
      throws IOException, SQLException;

  @Override
  public String toString() {
    return getClass().getName();
  }

  /** rowToText mode */
  public static ResponseGenerator rowToText(Map<String, String> config) {
    return new RowToText();
  }

  private static class RowToText extends ResponseGenerator {
    private static final String CONTENT_TYPE = "text/plain; charset=utf-8";
    private static final Charset ENCODING = Charset.forName("UTF-8");

    public RowToText() {
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
}
