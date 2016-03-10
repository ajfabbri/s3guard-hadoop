/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.web.resources;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

/**
 * Key value parameter passed part of the header string. Required for
 * analytics purpose in azure data lake backend.
 */
public abstract class ADLHeaderParam extends StringParam {

  ADLHeaderParam(Domain domain, String str) {
    super(domain, str);
  }

  public static String toSortedHeaderString(final String separator,
      final Param<?, ?>... parameters) {
    Arrays.sort(parameters, NAME_CMP);
    final StringBuilder b = new StringBuilder();
    try {
      for (Param<?, ?> p : parameters) {
        if (p.getValue() != null) {
          b.append(URLEncoder.encode(p.getName(), "UTF-8") + "=" + URLEncoder
              .encode(p.getValueString(), "UTF-8"));
          b.append(separator);
        }
      }
    } catch (UnsupportedEncodingException e) {
      //Suppress exception - Job execution is not impacted cause of this.
    }
    return b.toString();
  }

}
