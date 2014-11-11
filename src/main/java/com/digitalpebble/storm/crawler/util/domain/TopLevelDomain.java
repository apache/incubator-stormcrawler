/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.util.domain;

// Borrowed from Apache Nutch 1.9

public class TopLevelDomain extends DomainSuffix {

    public enum Type { INFRASTRUCTURE, GENERIC, COUNTRY };

    private Type type;
    private String countryName = null;

    public TopLevelDomain(String domain, Type type, Status status, float boost){
        super(domain, status, boost);
        this.type = type;
    }

    public TopLevelDomain(String domain, Status status, float boost, String countryName){
        super(domain, status, boost);
        this.type = Type.COUNTRY;
        this.countryName = countryName;
    }

    public Type getType() {
        return type;
    }

    /** Returns the country name if TLD is Country Code TLD
     * @return country name or null
     */
    public String getCountryName(){
        return countryName;
    }

}