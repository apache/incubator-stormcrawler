/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.protocol;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import java.util.List;

/**
 * Wrapper for BaseRobotRules which tracks the number of requests and length of the responses needed
 * to get the rules. If the array returned by getContentLengthFetched() is empty, then the rules
 * were obtained from the cache.
 */
public class RobotRules extends crawlercommons.robots.BaseRobotRules {

    private final BaseRobotRules base;
    private int[] bytesFetched = new int[] {};

    public RobotRules(BaseRobotRules base) {
        this.base = base;
    }

    @Override
    public boolean isAllowed(String url) {
        return base.isAllowed(url);
    }

    @Override
    public boolean isAllowAll() {
        return base.isAllowAll();
    }

    @Override
    public boolean isAllowNone() {
        return base.isAllowNone();
    }

    /** Returns the number of bytes fetched per request when not cached * */
    public int[] getContentLengthFetched() {
        return bytesFetched;
    }

    /** Returns the number of bytes fetched per request when not cached * */
    public void setContentLengthFetched(int[] bytesFetched) {
        this.bytesFetched = bytesFetched;
    }

    @Override
    public long getCrawlDelay() {
        return base.getCrawlDelay();
    }

    @Override
    public boolean isDeferVisits() {
        return base.isDeferVisits();
    }

    @Override
    public List<String> getSitemaps() {
        return base.getSitemaps();
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof BaseRobotRules)) return false;

        BaseRobotRules instance = (BaseRobotRules) obj;

        if (instance instanceof RobotRules) {
            // unwrapp the robot rules to assure that equals works.
            instance = ((RobotRules) instance).base;
        }

        return base.equals(instance);
    }

    @Override
    public void setCrawlDelay(long crawlDelay) {
        base.setCrawlDelay(crawlDelay);
    }

    @Override
    public void setDeferVisits(boolean deferVisits) {
        base.setDeferVisits(deferVisits);
    }

    @Override
    public void addSitemap(String sitemap) {
        base.addSitemap(sitemap);
    }

    public static void main(String[] args) {
        SimpleRobotRules wrappee = new SimpleRobotRules();
        RobotRules a = new RobotRules(wrappee);
        RobotRules b = new RobotRules(wrappee);
        System.out.println(a.equals(b));
    }
}
