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
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Metadata;
import com.google.enterprise.adaptor.Principal;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.TestHelper;
import com.google.enterprise.adaptor.UserPrincipal;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.GsaSpecialColumns;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** Test cases for {@link DatabaseAdaptor}. */
public class DatabaseAdaptorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  @Test
  public void testVerifyColumnNames_found() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testVerifyColumnNames_missing() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[foo] not found in query");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("id", "foo"));
  }

  @Test
  public void testVerifyColumnNames_lowercase() throws Exception {
    executeUpdate("create table data(id int, \"lower\" varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("LOWER"));
  }

  @Test
  public void testVerifyColumnNames_sqlException() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select from data", "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testVerifyColumnNames_nullMetaData() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "insert into data(id) values(42)",
        "found", Arrays.asList("id", "other"));
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

  /**
   * Returns a Database adaptor instance with the supplied config overrides.
   * Adaptor.initConfig() and Adaptor.init() have already been called.
   */
  private DatabaseAdaptor getObjectUnderTest(Map<String, String> moreEntries)
      throws Exception {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.driverClass", JdbcFixture.DRIVER_CLASS);
    configEntries.put("db.url", JdbcFixture.URL);
    configEntries.put("db.user", JdbcFixture.USER);
    configEntries.put("db.password", JdbcFixture.PASSWORD);
    configEntries.put("gsa.hostname", "localhost");
    configEntries.putAll(moreEntries);

    Config config = new Config();
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.initConfig(config);
    config.load(writeConfig(configEntries));
    adaptor.init(TestHelper.createConfigAdaptorContext(config));
    return adaptor;
  }

  /** Writes the config out as a Properties file and returns the File. */
  private File writeConfig(Map<String, String> config) throws IOException {
    File file = tempFolder.newFile("dba.test.properties");
    Properties properties = new Properties();
    properties.putAll(config);
    properties.store(new FileOutputStream(file), "");
    return file;
  }

  @Test
  public void testInitOfUrlMetadataListerNoDocIdIsUrl() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitOfUrlMetadataListerDocIdIsUrlFalse() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "false");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitOfUrlMetadataListerDocIdIsUrlTrue() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
    getObjectUnderTest(moreEntries);
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
  public void testInitVerifyColumnNames_uniqueKey() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[not_id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_singleDocContentSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.singleDocContentSqlParameters", "not_id");
    moreEntries.put("db.everyDocIdSql", "");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[not_id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_aclSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.aclSqlParameters", "not_id");
    moreEntries.put("db.everyDocIdSql", "");
    moreEntries.put("db.aclSql", "select other from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[not_id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_urlAndMetadata() throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.everyDocIdSql", "select url from data");
    moreEntries.put("db.metadataColumns", "other:other");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_actionColumn() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.actionColumn", "other");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_metadataColumns() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "");
    moreEntries.put("db.singleDocContentSql",
        "select id from data where id = ?");
    moreEntries.put("db.metadataColumns", "other:other");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_modeOfOperation() throws Exception {
    executeUpdate("create table data(\"must be set\" varchar, table_col int)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "contentColumn");
    moreEntries.put("db.modeOfOperation.contentColumn.columnName", "blob");
    moreEntries.put("db.uniqueKey", "table_col:int");
    moreEntries.put("db.everyDocIdSql", "");
    moreEntries.put("db.singleDocContentSql", "select * from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[blob] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testIncludeAllColumnsAsMetadata_mcBlank() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
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
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
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
    moreEntries.put("db.includeAllColumnsAsMetadata", "false");
    moreEntries.put("db.metadataColumns", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    assertEquals(asMap(), adaptor.metadataColumns);
  }

  @Test
  public void testIncludeAllColumnsAsMetadata_mcDefault() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    assertEquals(asMap(), adaptor.metadataColumns);
  }

  @Test
  public void testUniqueKeyMissingType() throws Exception {
    // Value of unique id cannot be "productid", because that is missing type.
    // The value has to be something like "productid:int"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey definition");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testUniqueKeyInvalidType() throws Exception {
    // Type of unique key id value cannot be "notvalid", since it's invalid.
    // That cat be int, string, timestamp, date, time, and long.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:notvalid");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey type 'notvalid'");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testUniqueKeyContainsRepeatedKeyName() throws Exception {
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int,productid:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("key name 'productid' was repeated");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testUniqueKeyEmpty() throws Exception {
    // Value of unique id cannot be empty.
    // The value has to be something like "keyname:type"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey parameter: value cannot be empty");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testUniqueKeyUrl() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "id:int, url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testIncludeAllColumnsAsMetadataFalse_mcSet() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
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
    moreEntries.put("db.metadataColumns", "fake column:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);

    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    thrown.expect(SQLException.class);
    thrown.expectMessage("Column \"fake column\" not found");
    adaptor.addMetadataToRecordBuilder(builder, resultSet);
  }

  @Test
  public void testCaseInsensitiveMetadataColumnMap() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "Foo:gsa_foo,Bar:gsa_bar");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "not_id:int");
    moreEntries.put("db.everyDocIdSql", "");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);

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
    executeUpdate("create table data(url varchar, name varchar)");
    executeUpdate("insert into data(url, name) values('http://', 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql", "");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "URL:col1, NAME:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "http://");
    metadata.add("col2",  "John");
    assertEquals(
        Arrays.asList(new Record.Builder(new DocId("http://"))
          .setMetadata(metadata).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIdsActionColumn() throws Exception {
    executeUpdate("create table data(id integer, url varchar, action varchar)");
    executeUpdate("insert into data(id, url, action) values"
        + "('1001', 'http://localhost/?q=1001', 'add'),"
        + "('1002', 'http://localhost/?q=1002', 'delete'),"
        + "('1003', 'http://localhost/?q=1003', 'DELETE'),"
        + "('1004', 'http://localhost/?q=1004', 'foo')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data order by url");
    configEntries.put("db.singleDocContentSql", "");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "id:id");
    configEntries.put("db.actionColumn", "action");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata1 = new Metadata();
    metadata1.add("id", "1001");
    Metadata metadata4 = new Metadata();
    metadata4.add("id", "1004");
    assertEquals(Arrays.asList(new Record[] {
        new Record.Builder(new DocId("http://localhost/?q=1001"))
            .setMetadata(metadata1).build(),
        new Record.Builder(new DocId("http://localhost/?q=1002"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1003"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1004"))
            .setMetadata(metadata4).build()}),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIdsActionColumnMissing() throws Exception {
    // Simulate a skipped column verification by creating the missing
    // column for init but removing it for getDocIds.
    executeUpdate("create table data(id integer, url varchar, action varchar)");
    executeUpdate("insert into data(id, url, action) values"
        + "(1001, 'http://localhost/?q=1001', 'add')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.actionColumn", "action");
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql", "");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "id:col1");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);

    executeUpdate("alter table data drop column action");

    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    thrown.expect(IOException.class);
    thrown.expectMessage("Column \"action\" not found");
    adaptor.getDocIds(pusher);
  }

  @Test
  public void testGetDocContent() throws Exception {
    executeUpdate("create table data(ID  integer, NAME  varchar)");
    executeUpdate("insert into data(ID, NAME) values('1001', 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "");
    configEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, NAME:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "1001");
    metadata.add("col2",  "John");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testClobColumnAsMetadata() throws Exception {
    // NCLOB shows up as CLOB in H2
    String content = "Hello World";
    executeUpdate("create table data(id int, content clob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.aclSqlParameters", "ID");
    configEntries.put("db.singleDocContentSqlParameters", "ID");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", content);
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testVarcharColumnAsMetadata() throws Exception {
    // LONGVARCHAR, LONGNVARCHAR show up as VARCHAR in H2.
    String content = "Hello World";
    executeUpdate("create table data(id int, content varchar)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.aclSqlParameters", "ID");
    configEntries.put("db.singleDocContentSqlParameters", "ID");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", content);
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testIntegerColumnAsMetadata() throws Exception {
    executeUpdate("create table data(id int, content integer)");
    executeUpdate("insert into data(id, content) values (1, 345697)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.aclSqlParameters", "ID");
    configEntries.put("db.singleDocContentSqlParameters", "ID");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", "345697");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testClobColumnWithNullMetadata() throws Exception {
    executeUpdate("create table data(id int, content clob)");
    executeUpdate("insert into data(id) values (1)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.aclSqlParameters", "ID");
    configEntries.put("db.singleDocContentSqlParameters", "ID");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", "null");
    assertEquals(expected, response.getMetadata());
  }
}
