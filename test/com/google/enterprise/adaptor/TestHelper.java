// Copyright 2011 Google Inc. All Rights Reserved.
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

package com.google.enterprise.adaptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for tests.
 *
 * <p>This code lives in adaptor package instead of adaptor.database package
 * to work around visibility of <code>Config</code> class.
 *
 * Copied from activedirectory repo at the same position.
 */
public class TestHelper {
  // Prevent instantiation
  private TestHelper() {}

  public static void setConfigValue(Config config, String key, String value) {
    config.setValue(key, value);
  }

  private static final SensitiveValueDecoder SENSITIVE_VALUE_DECODER
      = new SensitiveValueDecoder() {
    @Override
    public String decodeValue(String notEncodedDuringTesting) {
      return notEncodedDuringTesting;
    }
  };

  public static AdaptorContext createConfigAdaptorContext(final Config config) {
    return new WrapperAdaptor.WrapperAdaptorContext(null) {
      @Override
      public Config getConfig() {
        return config;
      }

      @Override
      public void setPollingIncrementalLister(PollingIncrementalLister lister) {
        // do nothing
      }

      @Override
      public SensitiveValueDecoder getSensitiveValueDecoder() {
        return SENSITIVE_VALUE_DECODER;
      }

      @Override
      public void setAuthzAuthority(AuthzAuthority authzAuthority) {
        // do nothing
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<T, T> asMap(T... entries) {
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException(
          "A map requires an even number of entries");
    }
    Map<T, T> map = new HashMap<>();
    for (int i = 0; i < entries.length - 1; i += 2) {
      map.put(entries[i], entries[i + 1]);
    }
    return map;
  }
}
