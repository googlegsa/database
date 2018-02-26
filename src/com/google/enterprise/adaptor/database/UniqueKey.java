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

import static java.util.Locale.US;

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.SqlType;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/** Provides adaptor DocId and fills getDocContent query. */
class UniqueKey {
  private static final Logger log
      = Logger.getLogger(UniqueKey.class.getName());

  static enum ColumnType {
    INT,
    LONG,
    BIGDECIMAL,
    STRING,
    TIMESTAMP,
    DATE,
    TIME
  }

  private final List<String> docIdSqlCols;  // columns used for DocId
  private final Map<String, ColumnType> types;  // types of DocId columns
  private final List<String> contentSqlCols;  // columns for content query
  private final List<String> aclSqlCols;  // columns for acl query
  private final List<String> metadataSqlCols; // columns for metadata query
  private final boolean docIdIsUrl;

  private UniqueKey(List<String> docIdSqlCols, Map<String, ColumnType> types,
      List<String> contentSqlCols, List<String> aclSqlCols,
      List<String> metadataSqlCols, boolean docIdIsUrl) {
    this.docIdSqlCols = docIdSqlCols;
    this.types = types;
    this.contentSqlCols = contentSqlCols;
    this.aclSqlCols = aclSqlCols;
    this.metadataSqlCols = metadataSqlCols;
    this.docIdIsUrl = docIdIsUrl;
  }

  String makeUniqueId(ResultSet rs) throws SQLException, URISyntaxException {
    if (docIdIsUrl) {
      // DocId must be a single string column.
      String urlStr = rs.getString(docIdSqlCols.get(0));
      return new ValidatedUri(urlStr).getUri().toString();
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < docIdSqlCols.size(); i++) {
      String name = docIdSqlCols.get(i);
      String part;
      switch (types.get(name)) {
        case INT:
          part = "" + rs.getInt(name);
          break;
        case LONG:
          part = "" + rs.getLong(name);
          break;
        case BIGDECIMAL:
          part = "" + rs.getBigDecimal(name);
          break;
        case STRING:
          part = rs.getString(name);
          break;
        case TIMESTAMP:
          Timestamp ts = rs.getTimestamp(name);
          part = (ts != null) ? "" + ts.getTime() : null;
          break;
        case DATE:
          part = "" + rs.getDate(name);
          break;
        case TIME:
          part = "" + rs.getTime(name);
          break;
        default:
          throw new AssertionError("Invalid type for column " + name
              + ": " + types.get(name));
      }
      if (rs.wasNull()) {
        throw new NullPointerException("Column \"" + name + "\" was null.");
      }
      sb.append("/").append(encodeSlashInData(part));
    }
    return sb.toString().substring(1);
  }

  private void setSqlValues(PreparedStatement st, String uniqueId,
      List<String> sqlCols) throws SQLException {
    Map<String, String> zip = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    // Parameters to SQL queries are taken from the unique id. When
    // reading extraMetadata when docIdIsUrl is true, we're now
    // passing the unique id to this method for the first time. Don't
    // try to split the id; we know it must be a single unique string.

    // TODO(aptls): while this may be OK in that we don't want to try
    // to split the id when we know it's a URL, the underlying use
    // case of using value(s) from the unique id to retrieve the extra
    // metadata might not want to have to use this URL as the metadata
    // key. We want to change this to be able to retrieve the metadata
    // key value(s) from the doc id or content result set instead of
    // the key.
    if (docIdIsUrl) {
      zip.put(docIdSqlCols.get(0), uniqueId);
    } else {
      // parse on / that isn't preceded by escape char _
      // (a / that is preceded by _ is part of column value)
      String parts[] = uniqueId.split("(?<!_)/", -1);
      if (parts.length != docIdSqlCols.size()) {
        String errmsg = "Wrong number of values for primary key: "
            + "id: " + uniqueId + ", parts: " + Arrays.asList(parts);
        throw new IllegalStateException(errmsg);
      }
      for (int i = 0; i < parts.length; i++) {
        String columnValue = decodeSlashInData(parts[i]);
        zip.put(docIdSqlCols.get(i), columnValue);
      }
    }
    for (int i = 0; i < sqlCols.size(); i++) {
      String colName = sqlCols.get(i);
      ColumnType typeOfCol = types.get(colName);
      String valueOfCol = zip.get(colName);
      switch (typeOfCol) {
        case INT:
          st.setInt(i + 1, Integer.parseInt(valueOfCol));
          break;
        case LONG:
          st.setLong(i + 1, Long.parseLong(valueOfCol));
          break;
        case BIGDECIMAL:
          st.setBigDecimal(i + 1, new BigDecimal(valueOfCol));
          break;
        case STRING:
          st.setString(i + 1, valueOfCol);
          break;
        case TIMESTAMP:
          long timestamp = Long.parseLong(valueOfCol);
          st.setTimestamp(i + 1, new java.sql.Timestamp(timestamp));
          break;
        case DATE:
          st.setDate(i + 1, java.sql.Date.valueOf(valueOfCol));
          break;
        case TIME:
          st.setTime(i + 1, java.sql.Time.valueOf(valueOfCol));
          break;
        default:
          throw new AssertionError("Invalid type for column " + colName
              + ": " + typeOfCol);
      }
    }
  }

  void setContentSqlValues(PreparedStatement st, String uniqueId)
      throws SQLException {
    setSqlValues(st, uniqueId, contentSqlCols);
  }

  void setAclSqlValues(PreparedStatement st, String uniqueId)
      throws SQLException {
    setSqlValues(st, uniqueId, aclSqlCols);
  }

  void setMetadataSqlValues(PreparedStatement st, String uniqueId)
      throws SQLException {
    setSqlValues(st, uniqueId, metadataSqlCols);
  }

  @VisibleForTesting
  static String encodeSlashInData(String data) {
    if (-1 == data.indexOf('/') && -1 == data.indexOf('_')) {
      return data;
    }
    char lastChar = data.charAt(data.length() - 1);
    // Don't let data end with _, because then a _ would
    // precede the seperator /.  If column value ends with
    // _ then append a / and take it away when decoding.
    // Since, column value could also end with / that we 
    // wouldn't want taken away during decoding, append a
    // second / when column value ends with / too.

    if ('_' == lastChar || '/' == lastChar) {
      // For _ case:
      // Suppose unique key values are 5_ and 6_. Without this code here
      // the unique keys would be encoded into DocId "5__/6__".  Then when
      // parsing DocId, the slash would not be used as a splitter because
      // it is preceded by _.
      // For / case:
      // Suppose unique key values are 5/ and 6/. Without appending another
      //  /, DocId will be 5_//6_/, which will be split and decoded as 5 
      // and 6.
      data += '/';
    }
    data = data.replace("_", "__");
    data = data.replace("/", "_/");
    return data;
  }

  @VisibleForTesting
  static String decodeSlashInData(String id) {
    // If id value ends with / (encoded as _/) we know that
    // this last / was appended because collumn value ended
    // with either _ or /. We take away this last added / 
    // character.
    if (id.endsWith("_/")) {
      id = id.substring(0, id.length() - 2);
    }
    id = id.replace("_/", "/");
    id = id.replace("__", "_");
    return id;
  }

  @Override
  public String toString() {
    return "UniqueKey(" + docIdSqlCols + "," + types + "," + contentSqlCols + ","
        + aclSqlCols + "," + metadataSqlCols + ")";
  }

  /** Builder to create instances of {@code UniqueKey}. */
  static class Builder {
    private final List<String> docIdSqlCols;  // columns used for DocId
    private Map<String, ColumnType> types;  // types of DocId columns
    private List<String> contentSqlCols;  // columns for content query
    private List<String> aclSqlCols;  // columns for acl query
    private List<String> metadataSqlCols;  // columns for metadata query
    private boolean docIdIsUrl = false;

    /**
     * Create a mutable builder for the UniqueKey configuration.
     *
     * @param docIdSqlColumns comma separated list of column names
     *     and optional types of the form: columnName[:type][, ...].
     *     If specified {@code type} must be one of
     *     {@code int, long, string, date, time, or timestamp}.
     * @throws InvalidConfigurationException if duplicate column names are
     *     provided, or an invalid column type is specified.
     * @throws NullPointerException if docIdSqlColumns is null
     */
    Builder(String docIdSqlColumns) throws InvalidConfigurationException {
      if (null == docIdSqlColumns) {
        throw new NullPointerException();
      }

      if ("".equals(docIdSqlColumns.trim())) {
        throw new InvalidConfigurationException(
            "Invalid db.uniqueKey parameter: value cannot be empty.");
      }

      List<String> tmpNames = new ArrayList<>();
      types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (String e : docIdSqlColumns.split(",", 0)) {
        log.fine("element: `" + e + "'");
        e = e.trim();
        String def[] = e.split(":", 2);
        String name = def[0].trim();
        ColumnType type;
        if (def.length == 1) {
          // No type given. Try to get type from ResultSetMetaData later.
          type = null;
        } else {
          String typeStr = def[1].trim();
          try {
            type = ColumnType.valueOf(typeStr.toUpperCase(US));
          } catch (IllegalArgumentException iae) {
            String errmsg = "Invalid UniqueKey type '" + typeStr
                + "' for '" + name + "'. Valid types are: "
                + Arrays.toString(ColumnType.values()).toLowerCase(US);
            throw new InvalidConfigurationException(errmsg);
          }
        }
        tmpNames.add(name);
        if (types.put(name, type) != null) {
          String errmsg = "Invalid db.uniqueKey configuration: key name '"
              + name + "' was repeated. Use a column alias if required.";
          throw new InvalidConfigurationException(errmsg);
        }
      }
      docIdSqlCols = Collections.unmodifiableList(tmpNames);
      contentSqlCols = docIdSqlCols;
      aclSqlCols = docIdSqlCols;
      metadataSqlCols = docIdSqlCols;
    }

    /**
     * Sets whether the DocId is a URL or not. If {@code true}, the
     * {@code docIdSqlColumns} supplied to the {@code Builder} must
     * consist of a single column of type {@code string}. If {@code false},
     * {@link #makeUniqueId} will encode DocIds composed of mulitple
     * column values, separated by slashes.
     *
     * @param docIdIsUrl if {@code true} the DocId is an URL
     */
    Builder setDocIdIsUrl(boolean docIdIsUrl) {
      this.docIdIsUrl = docIdIsUrl;
      return this;
    }

    /**
     * Sets the columns used by the {@code db.singleDocContentSql} query
     * parameters. The columms must also be included in the
     * {@code docIdSqlColumns} supplied to the {@code Builder} constructor.
     *
     * @param contentSqlColumns comma separated list of column names
     * @return this Builder
     * @throws InvalidConfigurationException if a column name is not found in
     *     the list of columns supplied to this Builder's constructor
     * @throws NullPointerException if contentSqlColumns is null
     */
    Builder setContentSqlColumns(String contentSqlColumns)
        throws InvalidConfigurationException {
      if (null == contentSqlColumns) {
        throw new NullPointerException();
      }
      if (!contentSqlColumns.trim().isEmpty()) {
        this.contentSqlCols
            = splitIntoNameList("db.singleDocContentSqlParameters",
                contentSqlColumns, types.keySet());
      }
      return this;
    }

    /**
     * Sets the columns used by the {@code db.aclSql} query parameters.
     * The columms must also be included in the
     * {@code docIdSqlColumns} supplied to the {@code Builder} constructor.
     *
     * @param aclSqlColumns comma separated list of column names
     * @return this Builder
     * @throws InvalidConfigurationException if a column name is not found in
     *     the list of columns supplied to this Builder's constructor.
     * @throws NullPointerException if aclSqlColumns is null
     */
    Builder setAclSqlColumns(String aclSqlColumns)
        throws InvalidConfigurationException {
      if (null == aclSqlColumns) {
        throw new NullPointerException();
      }
      if (!aclSqlColumns.trim().isEmpty()) {
        this.aclSqlCols = splitIntoNameList("db.aclSqlParameters",
            aclSqlColumns, types.keySet());
      }
      return this;
    }

    Builder setMetadataSqlColumns(String metadataSqlColumns)
        throws InvalidConfigurationException {
      if (metadataSqlColumns == null) {
        throw new NullPointerException();
      }
      if (!metadataSqlColumns.trim().isEmpty()) {
        this.metadataSqlCols =
            splitIntoNameList("db.extraMetadataSqlParameters",
                metadataSqlColumns, types.keySet());
      }
      return this;
    }

    private static List<String> splitIntoNameList(String paramConfig,
        String cols, Set<String> validNames) {
      List<String> columnNames = new ArrayList<String>();
      for (String name : cols.split(",", 0)) {
        name = name.trim();
        if (!validNames.contains(name)) {
          String errmsg = "Unknown column '" + name + "' from " + paramConfig
              + " not found in db.uniqueKey: " + validNames;
          throw new InvalidConfigurationException(errmsg);
        }
        columnNames.add(name);
      }
      return Collections.unmodifiableList(columnNames);
    }

    /**
     * Attempt to extract ColumnTypes from the supplied Map
     * of column names to java.sql.Type
     *
     * @param sqlTypes Map of column names to Integer java.sql.Types
     * @return this Builder
     * @throws InvalidConfigurationException if a column's SQL type cannot be
     *     mapped to a ColumnType.
     */
    Builder addColumnTypes(Map<String, SqlType> sqlTypes) {
      for (Map.Entry<String, SqlType> entry : sqlTypes.entrySet()) {
        if (types.get(entry.getKey()) != null) {
          continue;
        }
        ColumnType type;
        switch (entry.getValue().getType()) {
          case Types.BIT:
          case Types.BOOLEAN:
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
            type = ColumnType.INT;
            break;
          case Types.BIGINT:
            type = ColumnType.LONG;
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:
            type = ColumnType.BIGDECIMAL;
            break;
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.NCHAR:
          case Types.NVARCHAR:
          case Types.LONGNVARCHAR:
          case Types.DATALINK:
            type = ColumnType.STRING;
            break;
          case Types.DATE:
            type = ColumnType.DATE;
            break;
          case Types.TIME:
            type = ColumnType.TIME;
            break;
          case Types.TIMESTAMP:
            type = ColumnType.TIMESTAMP;
            break;
          default:
            String errmsg = "Invalid UniqueKey SQLtype '"
                + entry.getValue().getName() + "' for " + entry.getKey()
                + ". Please set an explicit key type in db.uniqueKey.";
            throw new InvalidConfigurationException(errmsg);
        }
        types.put(entry.getKey(), type);
      }
      return this;
    }

    /** Return the list of docIdSqlColumns. */
    List<String> getDocIdSqlColumns() {
      return docIdSqlCols;
    }

    /** Return the list of contentSqlColumns. */
    List<String> getContentSqlColumns() {
      return contentSqlCols;
    }

    /** Return the list of aclSqlColumns. */
    List<String> getAclSqlColumns() {
      return aclSqlCols;
    }

    List<String> getMetadataSqlColumns() {
      return metadataSqlCols;
    }

    /** Return the Map of column types. */
    @VisibleForTesting
    Map<String, ColumnType> getColumnTypes() {
      return Collections.unmodifiableMap(types);
    }

    /**
     * @return a {@code UniqueKey} instance constructed with configuration
     *   supplied to this Builder.
     */
    UniqueKey build() throws InvalidConfigurationException {
      ArrayList<String> badColumns = new ArrayList<>();
      for (Map.Entry<String, ColumnType> entry : types.entrySet()) {
        if (entry.getValue() == null) {
          badColumns.add(entry.getKey());
        }
      }
      if (!badColumns.isEmpty()) {
        throw new InvalidConfigurationException("Unknown column type for the"
            + " following columns: " + badColumns
            + ". Please set explicit types in db.uniqueKey.");
      }
      types = Collections.unmodifiableMap(types);
      if (docIdIsUrl
          && (types.size() != 1
              || types.values().iterator().next() != ColumnType.STRING)) {
        throw new InvalidConfigurationException("Invalid db.uniqueKey value:"
           + " The key must be a single string column when docId.isUrl=true.");
      }
      return new UniqueKey(docIdSqlCols, types, contentSqlCols, aclSqlCols,
          metadataSqlCols, docIdIsUrl);
    }
  }
}
