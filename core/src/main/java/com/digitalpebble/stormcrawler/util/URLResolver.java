/*
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
package com.digitalpebble.stormcrawler.util;

import java.net.*;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Describes the strategies used by the RemoteDriverProtocol to resolve the given addresses. This is
 * sometimes necessary, when running in a docker container or special environment.
 */
// TODO: Right now it only resolves to the first possible target. Maybe we need some kind of
//  regex-pattern-matching for multiple resolved addresses at some point?
public enum URLResolver {
    NOTHING {
        @NotNull
        @Override
        public URL resolveDirect(@NotNull URL url) {
            return url;
        }

        @Override
        public ResolvedUrl resolve(@NotNull URL url) {
            return ResolvedUrl.of(url);
        }

        @Contract("_ -> fail")
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(
                    "Nothing does literally nothing else than returning the same value.");
        }
    },
    IP {
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    IPv4 {
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = Inet4Address.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    /** Only resolves if IPv6 is possible. Otherwise, uses IPv4. */
    IPv6 {
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = Inet6Address.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    HOSTNAME {
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getHostName();
        }
    },
    CANONICAL_HOSTNAME {
        @NotNull
        @Override
        protected String resolveHostAddress(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getCanonicalHostName();
        }
    };

    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(URLResolver.class);

    /** A simple pair of original and resolved url. */
    public static final class ResolvedUrl {
        private final @NotNull URL origin;
        private final @Nullable URL resolved;

        public ResolvedUrl(@NotNull URL origin, @Nullable URL resolved) {
            this.origin = origin;
            this.resolved = resolved;
        }

        /**
         * Creates a {@code URLTuple} with the given {@code origin} as {@code origin} and {@code
         * resolved}.
         */
        @NotNull
        public static ResolvedUrl of(@NotNull URL origin) {
            return new ResolvedUrl(origin, origin);
        }

        /** Creates a {@code URLTuple} with the given {@code origin} and {@code resolved} URL. */
        @NotNull
        public static ResolvedUrl of(@NotNull URL origin, @Nullable URL resolved) {
            return new ResolvedUrl(origin, resolved);
        }

        @NotNull
        public URL getOrigin() {
            return origin;
        }

        @Nullable
        public URL getResolved() {
            return resolved;
        }

        /**
         * Returns either the {@link ResolvedUrl#resolved} url or the {@link ResolvedUrl#origin} if
         * the resolving failed.
         */
        @NotNull
        public URL getResolvedOrOrigin() {
            if (resolved != null) return resolved;
            return origin;
        }

        /** Returns true, if resolved is not null. */
        @Contract(pure = true)
        public boolean wasSuccessfullyResolved() {
            return resolved != null;
        }

        /** Returns true, if resolved is null due to failed resolving. */
        @Contract(pure = true)
        public boolean wasNotSuccessfullyResolved() {
            return resolved == null;
        }

        @Override
        public String toString() {
            return "ResolvedUrl{" + "origin=" + origin + ", resolved=" + resolved + '}';
        }
    }

    /**
     * Tries to resolve the given {@code url} to a specific target. Returns {@code null} if it can
     * not resolve the {@code url}.
     */
    @Nullable
    public URL resolveDirect(@NotNull URL url) {
        String replacement = null;
        try {
            replacement = resolveHostAddress(url);
            return new URL(url.getProtocol(), replacement, url.getPort(), url.getFile());
        } catch (UnknownHostException e) {
            LOG.warn("Was not able to resolve the address {} to {}.", url.toExternalForm(), name());
            return null;
        } catch (MalformedURLException e) {
            LOG.warn(
                    "Was not able to create the new URL from {} with {} in {}.",
                    url.toExternalForm(),
                    replacement,
                    name());
            return null;
        }
    }

    /**
     * Tries to resolve the given {@code url} to a specific target. Always returns a {@link
     * ResolvedUrl} instance.
     */
    public ResolvedUrl resolve(@NotNull URL url) {
        return ResolvedUrl.of(url, resolveDirect(url));
    }

    /** Returns a string used as host-part in the new url. */
    @NotNull
    protected abstract String resolveHostAddress(@NotNull URL url) throws UnknownHostException;
}
