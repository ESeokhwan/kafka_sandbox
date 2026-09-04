/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

public class DynamicTagsFilter extends Filter {

    private String[] tags;

    public static String tagPrefix(String tag) {
        return "[tag: " + tag + "] ";
    }

    public void setTags(String tagsString) {
        this.tags = tagsString.split(",");
    }

    public int decide(LoggingEvent event) {
        if (tags == null || event.getMessage() == null) {
            return Filter.NEUTRAL;
        }

        String message = event.getRenderedMessage();
        if (!(message.startsWith("[tag: ") && message.contains("] "))) {
            return ACCEPT;
        }

        for (String tag: tags) {
            if (isFilteredByTag(tag, message)) {
                return ACCEPT;
            }
        }
        return DENY;
    }

    private boolean isFilteredByTag(String tag, String message) {
        if (message == null) {
            return false;
        }
        return message.startsWith(tagPrefix(tag));
    }
}
