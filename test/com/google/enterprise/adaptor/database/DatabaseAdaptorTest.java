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

import static org.junit.Assert.*;

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.TestHelper;
import com.google.enterprise.adaptor.UserPrincipal;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.GsaSpecialColumns;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Test cases for {@link DatabaseAdaptor}. */
public class DatabaseAdaptorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLoadResponseGeneratorWithFactoryMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "rowToText");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorWithFullyQualifiedMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    String modeOfOperation = "com.google.enterprise.adaptor.database"
        + ".DatabaseAdaptorTest.createDummy";
    configEntries.put("db.modeOfOperation", modeOfOperation);
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchFactoryMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "noThisMethod");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }

    thrown.expect(InvalidConfigurationException.class);
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClassMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation",
        "com.google.enterprise.adaptor.database"
            + ".DatabaseAdaptorTest.noThisMode");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }

    thrown.expect(InvalidConfigurationException.class);
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClass() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "noThisClass.noThisMethod");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }

    thrown.expect(InvalidConfigurationException.class);
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static ResponseGenerator createDummy(Map<String, String> config) {
    return new DummyResponseGenerator(config);
  }

  private static class DummyResponseGenerator extends ResponseGenerator {
    public DummyResponseGenerator(Map<String, String> config) {
      super(config);
    }

    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      // do nothing
    }
  }
  
  @Test
  public void testAclSqlResultSetHasNoRecord() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = Acl.EMPTY;
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");
    
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetHasOneRecord() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser1, puser2");
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1, pgroup2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup2");
    mockResultSet.addRecord(record);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser1"),
            new UserPrincipal("duser2")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup2"),
            new GroupPrincipal("dgroup1")))
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");
    
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetWithInheritFrom() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser1, puser2");
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1, pgroup2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup2");
    record.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(), "parent");
    record.put(GsaSpecialColumns.GSA_INHERITANCE_TYPE.toString(),
        "PARENT_OVERRIDES");
    mockResultSet.addRecord(record);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser1"),
            new UserPrincipal("duser2")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup2"),
            new GroupPrincipal("dgroup1")))
        .setInheritFrom(new DocId("parent"))
        .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDES)
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");

    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetWithMultipleInheritFrom() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record1 = new TreeMap<String, String>();
    record1.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(), "parent1");
    mockResultSet.addRecord(record1);
    Map<String, String> record2 = new TreeMap<String, String>();
    record2.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(), "parent2");
    mockResultSet.addRecord(record2);
    Map<String, String> record3 = new TreeMap<String, String>();
    record3.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(), "PARENT1");
    mockResultSet.addRecord(record3);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    thrown.expect(IllegalStateException.class);
    DatabaseAdaptor.buildAcl(rs, metadata, ",");
  }

  @Test
  public void testAclSqlResultSetWithMultipleInheritanceType()
      throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record1 = new TreeMap<String, String>();
    record1.put(GsaSpecialColumns.GSA_INHERITANCE_TYPE.toString(),
        "PARENT_OVERRIDES");
    mockResultSet.addRecord(record1);
    Map<String, String> record2 = new TreeMap<String, String>();
    record2.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(),
        "PARENT_OVERRIDES");
    mockResultSet.addRecord(record2);
    Map<String, String> record3 = new TreeMap<String, String>();
    record3.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(), "LEAF_NODE");
    mockResultSet.addRecord(record3);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    thrown.expect(IllegalStateException.class);
    DatabaseAdaptor.buildAcl(rs, metadata, ",");
  }

  @Test
  public void testAclSqlResultSetWithOverlappingInheritanceData()
      throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record1 = new TreeMap<String, String>();
    record1.put(GsaSpecialColumns.GSA_INHERITANCE_TYPE.toString(),
        "PARENT_OVERRIDES");
    record1.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(),
        "ROOT");
    mockResultSet.addRecord(record1);
    Map<String, String> record2 = new TreeMap<String, String>();
    record2.put(GsaSpecialColumns.GSA_INHERITANCE_TYPE.toString(),
        "PARENT_OVERRIDES");
    record2.put(GsaSpecialColumns.GSA_INHERIT_FROM.toString(),
        "ROOT");
    mockResultSet.addRecord(record2);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = new Acl.Builder()
        .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDES)
        .setInheritFrom(new DocId("ROOT"))
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");

    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasNonOverlappingTwoRecord()
      throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser1, puser2");
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1, pgroup2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup2");
    mockResultSet.addRecord(record);
    Map<String, String> record2 = new TreeMap<String, String>();
    record2.put(
        GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser3, puser4");
    record2.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser3, duser4");
    record2.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup3, pgroup4");
    record2.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup3, dgroup4");
    mockResultSet.addRecord(record2);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1"),
            new UserPrincipal("puser4"),
            new UserPrincipal("puser3")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser2"),
            new UserPrincipal("duser1"),
            new UserPrincipal("duser4"),
            new UserPrincipal("duser3")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup3"),
            new GroupPrincipal("pgroup4"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup1"),
            new GroupPrincipal("dgroup3"),
            new GroupPrincipal("dgroup4"),
            new GroupPrincipal("dgroup2")))
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");
    
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetHasOverlappingTwoRecord()
      throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser1, puser2");
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1, pgroup1");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup1");
    mockResultSet.addRecord(record);
    Map<String, String> record2 = new TreeMap<String, String>();
    record2.put(
        GsaSpecialColumns.GSA_PERMIT_USERS.toString(), "puser2, puser1");
    record2.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser4, duser2");
    record2.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1");
    record2.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup2, dgroup2");
    mockResultSet.addRecord(record2);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    ResultSetMetaData metadata = getResultSetMetaDataHasAllAclColumns();

    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser2"),
            new UserPrincipal("duser1"),
            new UserPrincipal("duser4")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup1"),
            new GroupPrincipal("dgroup2")))
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");
    
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetOneColumnMissing() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_PERMIT_GROUPS.toString(), "pgroup1, pgroup2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup2");
    mockResultSet.addRecord(record);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    
    MockAclSqlResultSetMetaData mockMetadata =
        new MockAclSqlResultSetMetaData();
    mockMetadata.addColumn(GsaSpecialColumns.GSA_DENY_USERS.toString());
    mockMetadata.addColumn(GsaSpecialColumns.GSA_PERMIT_GROUPS.toString());
    mockMetadata.addColumn(GsaSpecialColumns.GSA_DENY_GROUPS.toString());

    ResultSetMetaData metadata =
        (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] {ResultSetMetaData.class}, mockMetadata);

    Acl golden = new Acl.Builder()
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser1"),
            new UserPrincipal("duser2")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup2"),
            new GroupPrincipal("dgroup1")))
        .build();
    Acl acl = DatabaseAdaptor.buildAcl(rs, metadata, ",");
    
    assertEquals(golden, acl);
  }
  
  @Test
  public void testGetPrincipalsWithEmptyDelim() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1, duser2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1, dgroup2");
    mockResultSet.addRecord(record);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    rs.next();
    
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1, duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1, dgroup2"));

    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, "");
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, "");
    
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }
  
  @Test
  public void testGetPrincipalsWithUserDefinedDelim() throws SQLException {
    MockAclSqlResultSet mockResultSet = new MockAclSqlResultSet();
    Map<String, String> record = new TreeMap<String, String>();
    record.put(GsaSpecialColumns.GSA_DENY_USERS.toString(), "duser1 ; duser2");
    record.put(
        GsaSpecialColumns.GSA_DENY_GROUPS.toString(), "dgroup1 ; dgroup2");
    mockResultSet.addRecord(record);

    ResultSet rs =
        (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
            new Class[] {ResultSet.class}, mockResultSet);
    rs.next();
    
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1"), 
        new UserPrincipal("duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1"),
        new GroupPrincipal("dgroup2"));

    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, " ; ");
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, " ; ");
    
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }
  
  private static class MockAclSqlResultSet implements InvocationHandler {
    private ArrayList<Map<String, String>> records;
    private int cursor;
    
    public MockAclSqlResultSet() {
      records = new ArrayList<Map<String, String>>();
      cursor = -1;
    }
    
    public void addRecord(Map<String, String> record) {
      records.add(record);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName(); 
      if ("getString".equals(methodName)) {
        return records.get(cursor).get(args[0]);
      } else if ("next".equals(methodName)) {
        cursor++;
        return cursor < records.size();
      } else {
        throw new IllegalStateException("unexpected call: " + methodName);
      }
    }
  }
  
  private static class MockAclSqlResultSetMetaData
      implements InvocationHandler {
    private ArrayList<String> columns;
    
    public MockAclSqlResultSetMetaData() {
      columns = new ArrayList<String>();
      columns.add(""); // ResultSetMetaData column index starts from 1
    }
    
    public void addColumn(String column) {
      columns.add(column);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName(); 
      if ("getColumnCount".equals(methodName)) {
        return columns.size() - 1; // don't count the first empty string
      } else if ("getColumnName".equals(methodName)) {
        return columns.get((Integer) args[0]);
      } else {
        throw new IllegalStateException("unexpected call: " + methodName);
      }
    }
  }

  private static ResultSetMetaData getResultSetMetaDataHasAllAclColumns() {
    MockAclSqlResultSetMetaData mockMetadata =
        new MockAclSqlResultSetMetaData();
    for (GsaSpecialColumns column : GsaSpecialColumns.values()) {
      mockMetadata.addColumn(column.toString());
    }

    return (ResultSetMetaData) Proxy.newProxyInstance(
        ResultSetMetaData.class.getClassLoader(),
        new Class[] {ResultSetMetaData.class}, mockMetadata);
  }
}
