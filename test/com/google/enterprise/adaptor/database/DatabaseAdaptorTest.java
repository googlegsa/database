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

import static com.google.enterprise.adaptor.DocIdPusher.Record;
import static com.google.enterprise.adaptor.Principal.DEFAULT_NAMESPACE;
import static com.google.enterprise.adaptor.TestHelper.asMap;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQuery;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Metadata;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.TestHelper;
import com.google.enterprise.adaptor.UserPrincipal;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.GsaSpecialColumns;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link DatabaseAdaptor}. */
public class DatabaseAdaptorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

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
  public void testLoadResponseGeneratorWithBuiltinFullyQualifiedMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    String modeOfOperation = "com.google.enterprise.adaptor.database"
        + ".ResponseGenerator.rowToText";
    configEntries.put("db.modeOfOperation", modeOfOperation);
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorWithCustomFullyQualifiedMethod() {
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
    thrown.expectMessage(
        "noThisMethod is not a valid built-in modeOfOperation");
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
    thrown.expectMessage("No method noThisMode found for class "
        + "com.google.enterprise.adaptor.database.DatabaseAdaptorTest");
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

  @Test
  public void testInitOfUrlMetadataListerNoDocIdIsUrl() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.modeOfOperation.urlAndMetadataLister.columnName", "ur");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testInitOfUrlMetadataListerDocIdIsUrlFalse() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.modeOfOperation.urlAndMetadataLister.columnName", "ur");
    moreEntries.put("docId.isUrl", "false");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testInitOfUrlMetadataListerDocIdIsUrlTrue() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.modeOfOperation.urlAndMetadataLister.columnName", "ur");
    moreEntries.put("docId.isUrl", "true");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.initConfig(config);
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  private Config createStandardConfig(Map<String, String> moreEntries) {
    Map<String, String> configEntries = new HashMap<String, String>();
    // driverClass must be specified, but (other than it pointing at a valid
    // class), the value does not matther.
    configEntries.put("db.driverClass",
        "com.google.enterprise.adaptor.database.DatabaseAdaptor");
    configEntries.put("db.url", "must be set");
    configEntries.put("db.user", "must be set");
    configEntries.put("db.password", "must be set");
    configEntries.put("db.uniqueKey", "must be set:string");
    configEntries.put("db.everyDocIdSql", "must be set");
    configEntries.put("db.singleDocContentSqlParameters", "must be set");
    configEntries.put("db.singleDocContentSql", "must be set");
    configEntries.put("db.aclSqlParameters", "must be set");
    configEntries.put("db.includeAllColumnsAsMetadata", "false");
    configEntries.put("db.metadataColumns", "table_col:gsa_col");
    // configEntries.put("db.aclSql", "table_col:gsa_col");
    configEntries.put("db.aclSql", "");
    configEntries.put("db.aclPrincipalDelimiter", ",");
    configEntries.put("db.disableStreaming", "true");
    configEntries.put("db.updateTimestampTimezone", "");
    configEntries.put("db.updateSql", "");
    configEntries.putAll(moreEntries);
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    return config;
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
    Acl golden = Acl.EMPTY;

    executeUpdate("create table acl");
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasOneRecord() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1, pgroup2', 'puser1, puser2', "
        + "'dgroup1, dgroup2', 'duser1, duser2')");
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
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasNonOverlappingTwoRecord()
      throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1, pgroup2', 'puser1, puser2', "
        + "'dgroup1, dgroup2', 'duser1, duser2')");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup3, pgroup4', 'puser3, puser4', "
        + "'dgroup3, dgroup4', 'duser3, duser4')");
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
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetHasOverlappingTwoRecord()
      throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1, pgroup1', 'puser1, puser2', "
        + "'dgroup1, dgroup1', 'duser1, duser2')");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1', 'puser2, puser1', "
        + "'dgroup2, dgroup2', 'duser4, duser2')");
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
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetOneColumnMissing() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1, pgroup2', "
        + "'dgroup1, dgroup2', 'duser1, duser2')");
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
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testGetPrincipalsWithEmptyDelim() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'dgroup1, dgroup2', 'duser1, duser2')");
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1, duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1, dgroup2"));
    ResultSet rs = executeQueryAndNext("select * from acl");
    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, "", DEFAULT_NAMESPACE);
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, "", DEFAULT_NAMESPACE);
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }
  
  @Test
  public void testGetPrincipalsWithUserDefinedDelim() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'dgroup1 ; dgroup2', 'duser1 ; duser2')");
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1"), 
        new UserPrincipal("duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1"),
        new GroupPrincipal("dgroup2"));
    ResultSet rs = executeQueryAndNext("select * from acl");
    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, " ; ", DEFAULT_NAMESPACE);
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, " ; ", DEFAULT_NAMESPACE);
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }

  @Test
  public void testIncludeAllColumnsAsMetadata_mcBlank() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
    assertEquals(null, adaptor.metadataColumns);

    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("ID", "1");
    golden.add("FOO", "fooVal");
    golden.add("BAR", "barVal");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testIncludeAllColumnsAsMetadata_mcSet() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
    assertEquals(asMap("db_col1", "gsa1", "col2", "gsa2"),
                 adaptor.metadataColumns);

    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa1", "col1Val");
    golden.add("gsa2", "col2Val");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testIncludeAllColumnsAsMetadataFalse_mcBlank() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
    assertEquals(asMap(), adaptor.metadataColumns);
  }

  @Test
  public void testUniqueKeyMissingType() throws Exception {
    // Value of unique id cannot be "productid", because that is missing type.
    // The value has to be something like "productid:int"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "productid");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey definition");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testUniqueKeyInvalidType() throws Exception {
    // Type of unique key id value cannot be "notvalid", since it's invalid.
    // That cat be int, string, timestamp, date, time, and long.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "productid:notvalid");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey type 'notvalid'");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testUniqueKeyContainsRepeatedKeyName() throws Exception {
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "productid:int,productid:string");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("key name 'productid' was repeated");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testUniqueKeyEmpty() throws Exception {
    // Value of unique id cannot be empty.
    // The value has to be something like "keyname:type"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey parameter: value cannot be empty");
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
  }

  @Test
  public void testIncludeAllColumnsAsMetadataFalse_mcSet() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
    assertEquals(asMap("db_col1", "gsa1", "col2", "gsa2"),
                 adaptor.metadataColumns);

    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa1", "col1Val");
    golden.add("gsa2", "col2Val");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInvalidColumnAsMetadata() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "fake column:gsa1,col2:gsa2");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));

    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    thrown.expect(SQLException.class);
    adaptor.addMetadataToRecordBuilder(builder, resultSet);
  }

  @Test
  public void testCaseInsensitiveMetadataColumnMap() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("adaptor.namespace", "Default");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "Foo:gsa_foo,Bar:gsa_bar");
    final Config config = createStandardConfig(moreEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));

    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa_foo", "fooVal");
    golden.add("gsa_bar", "barVal");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testGetDocIds() throws Exception {
    executeUpdate("create table data(ID  integer, NAME  varchar)");
    executeUpdate("insert into data(ID, NAME) values('1001', 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.user", "sa");
    configEntries.put("db.password", "");
    configEntries.put("db.url", JdbcFixture.URL);
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql", "");
    configEntries.put("db.singleDocContentSqlParameters", "");
    configEntries.put("db.aclSqlParameters", "ID");
    configEntries.put("adaptor.namespace", "Default");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("db.modeOfOperation.urlAndMetadataLister.columnName",
        "ID");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "ID:col1, NAME:col2");

    Config config = createStandardConfig(configEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));

    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "1001");
    metadata.add("col2",  "John");
    assertEquals(
        Arrays.asList(new Record.Builder(new DocId("1001"))
          .setMetadata(metadata).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocContent() throws Exception {
    executeUpdate("create table data(ID  integer, NAME  varchar)");
    executeUpdate("insert into data(ID, NAME) values('1001', 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.user", "sa");
    configEntries.put("db.password", "");
    configEntries.put("db.url", JdbcFixture.URL);
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    configEntries.put("adaptor.namespace", "Default");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, NAME:col2");

    Config config = createStandardConfig(configEntries);
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.init(TestHelper.createConfigAdaptorContext(config));

    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "1001");
    metadata.add("col2",  "John");
    assertEquals(metadata, response.getMetadata());
  }
}
