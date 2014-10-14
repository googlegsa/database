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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.TestHelper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Tests for DatabaseAdaptor functionality */
public class DatabaseAdaptorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLoadResponseGeneratorWithFactoryMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "rowToText");
    final Config config = new Config();
    for (Map.Entry<String, String> entry: configEntries.entrySet()) {
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
    for (Map.Entry<String, String> entry: configEntries.entrySet()) {
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
    for (Map.Entry<String, String> entry: configEntries.entrySet()) {
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
    for (Map.Entry<String, String> entry: configEntries.entrySet()) {
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
    for (Map.Entry<String, String> entry: configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }

    thrown.expect(InvalidConfigurationException.class);
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static ResponseGenerator createDummy(Map<String, String> config) {
    return new DummyResponseGenerator();
  }

  private static class DummyResponseGenerator extends ResponseGenerator {
    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      // do nothing
    }
  }
}
