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

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.adaptor.InvalidConfigurationException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/** Provides adaptor DocId and fills getDocContent query. */
class UniqueKey {
  private static final Logger log
      = Logger.getLogger(UniqueKey.class.getName());

  static enum ColumnType {
    INT,
    STRING
  }

  private final List<String> names;  // columns used for DocId
  private final Map<String, ColumnType> types;  // types of DocId columns
  private final List<String> contentSqlCols;  // columns for content query

  UniqueKey(String pkDecls, String contentSqlColumns) {
    if (null == pkDecls) {
      throw new NullPointerException();
    }
    if (null == contentSqlColumns) {
      throw new NullPointerException();
    }

    if ("".equals(pkDecls.trim())) {
      throw new InvalidConfigurationException("empty");
    }

    List<String> tmpNames = new ArrayList<String>();
    Map<String, ColumnType> tmpTypes = new TreeMap<String, ColumnType>();
    for (String e : pkDecls.split(",", -1)) {
      log.fine("element: " + e);
      String def[] = e.split(":", -1);
      if (2 != def.length) {
        throw new InvalidConfigurationException("bad def: " + e);
      }
      String name = def[0];
      ColumnType type;
      if ("int".equals(def[1].toLowerCase())) {
        type = ColumnType.INT;
      } else if ("string".equals(def[1].toLowerCase())) {
        type = ColumnType.STRING;
      } else {
        throw new InvalidConfigurationException("bad type: " + def[1]);
      } 
      if (tmpTypes.containsKey(name)) {
        throw new InvalidConfigurationException("name repeat: " + name);
      }
      tmpNames.add(name);
      tmpTypes.put(name, type);
    }
    names = Collections.unmodifiableList(tmpNames);
    types = Collections.unmodifiableMap(tmpTypes);

    if ("".equals(contentSqlColumns.trim())) {
      contentSqlCols = names;
      return;
    }

    List<String> tmpContentCols = new ArrayList<String>();
    for (String e : contentSqlColumns.split(",", -1)) {
      String name = e;
      if (!tmpTypes.containsKey(name)) {
        throw new InvalidConfigurationException("unknown name: " + name);
      }
      tmpContentCols.add(name); 
    }
    contentSqlCols = Collections.unmodifiableList(tmpContentCols);
  }

  @VisibleForTesting
  UniqueKey(String pkDecls) {
    this(pkDecls, "");
  }

  public String toString() {
    return "UniqueKey(" + names + "," + types + "," + contentSqlCols + ")";
  }

  String makeUniqueId(ResultSet rs) throws SQLException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      String part;
      switch (types.get(name)) {
        case INT:
          part = "" + rs.getInt(name);
          break;
        case STRING:
          part = rs.getString(name);
          break;
        default:
          throw new AssertionError("invalid type: " + types.get(name)); 
      } 
      part = encodeSlashInData(part);
      sb.append("/").append(part);
    }
    return sb.toString().substring(1);
  }

  void setStatementValues(PreparedStatement st, String uniqueId)
      throws SQLException {
    uniqueId = decodeSlashInData(uniqueId);
    String parts[] = uniqueId.split("/", -1);
    if (parts.length != names.size()) {
      throw new IllegalStateException("wrong number of values for primary key");
    }

    Map<String, String> zip = new TreeMap<String, String>();
    for (int i = 0; i < parts.length; i++) {
      zip.put(names.get(i), parts[i]);
    }

    for (int i = 0; i < contentSqlCols.size(); i++) {
      String colName = contentSqlCols.get(i);
      ColumnType typeOfCol = types.get(colName);
      String valueOfCol = zip.get(colName);
      switch (typeOfCol) {
        case INT:
          st.setInt(i + 1, Integer.parseInt(valueOfCol));
          break;
        case STRING:
          st.setString(i + 1, valueOfCol);
          break;
        default:
          throw new AssertionError("invalid type: " + typeOfCol); 
      }
    }   
  }

  @VisibleForTesting
  static String encodeSlashInData(String data) {
    if (-1 == data.indexOf('/') && -1 == data.indexOf('\\')) {
      return data;
    }
    data = data.replace("\\", "\\\\");
    data = data.replace("/", "\\/");
    return data; 
  }

  @VisibleForTesting
  static String decodeSlashInData(String id) {
    id = id.replace("\\/", "/");
    id = id.replace("\\\\", "\\");
    return id;
  }

  /** Number of columns that make up the primary key. */
  @VisibleForTesting
  int numElementsForTest() {
    return names.size(); 
  }

  /** Name of particular column in primary key. */
  @VisibleForTesting
  String nameForTest(int i) {
    return names.get(i);
  }

  /** Type of particular column in primary key. */
  @VisibleForTesting
  ColumnType typeForTest(int i) {
    return types.get(names.get(i));
  }
}
