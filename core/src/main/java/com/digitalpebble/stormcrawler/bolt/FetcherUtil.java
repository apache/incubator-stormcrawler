package com.digitalpebble.stormcrawler.bolt;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import crawlercommons.domains.PaidLevelDomain;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

public final class FetcherUtil {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FetcherUtil.class);

    private FetcherUtil() {}

    /** Default config key for the queue mode */
    public static final String defaultFetcherModeKey = "fetcher.queue.mode";
    /** Default fallback value for the queue mode */
    public static final QueueMode defaultFallbackQueueMode = QueueMode.QUEUE_MODE_HOST;

    /**
     * Reads the {@link QueueMode} from the given {@code stormConf} with the provided {@code
     * fetcherQueueModeKey}. If the value at {@code fetcherQueueModeKey} is null or invalid it
     * returns the {@code fallback}.
     *
     * @param stormConf reference to the config.
     * @param fetcherQueueModeKey config key to the queue mode.
     * @param fallback fallback value if the value at {@code fetcherQueueModeKey} is not valid.
     * @return the extracted {@link QueueMode}
     */
    @NotNull
    public static QueueMode readQueueMode(
            @NotNull Map<String, Object> stormConf,
            @NotNull String fetcherQueueModeKey,
            @NotNull QueueMode fallback) {
        String partitionMode = ConfUtils.getString(stormConf, fetcherQueueModeKey);
        QueueMode queueMode = QueueMode.parseQueueModeLabel(partitionMode);

        if (queueMode == null) {
            LOG.error("Unknown partition mode : {} - forcing to {}", partitionMode, fallback.label);
            queueMode = fallback;
        }

        return queueMode;
    }

    /**
     * Reads the {@link QueueMode} from the given {@code stormConf} with the default value {@value
     * defaultFetcherModeKey} and {@code defaultFallbackQueueMode}.
     *
     * @param stormConf reference to the config
     * @return the extracted {@link QueueMode}
     */
    @NotNull
    public static QueueMode readQueueMode(@NotNull Map<String, Object> stormConf) {
        return readQueueMode(stormConf, defaultFetcherModeKey, defaultFallbackQueueMode);
    }

    /**
     * Gets the key for the fetcher queue from the given {@code url} by the provided {@code
     * queueMode}.
     *
     * @param url used for the key extraction.
     * @param queueMode of the fetcher.
     * @return a string value from the {@code url} for a queue.
     */
    @NotNull
    public static String getQueueKeyByMode(@NotNull URL url, @NotNull QueueMode queueMode) {
        // Origin: FetchItem.create

        String key = null;

        switch (queueMode) {
            case QUEUE_MODE_IP:
                try {
                    final InetAddress addr = InetAddress.getByName(url.getHost());
                    key = addr.getHostAddress();
                } catch (final UnknownHostException e) {
                    LOG.warn("Unable to resolve IP for {}, using hostname as key.", url.getHost());
                    key = url.getHost();
                }
                break;
            case QUEUE_MODE_DOMAIN:
                key = PaidLevelDomain.getPLD(url.getHost());
                if (key == null) {
                    LOG.warn("Unknown domain for url: {}, using hostname as key", url);
                    key = url.getHost();
                }
                break;
            case QUEUE_MODE_HOST:
                key = url.getHost();
                break;
        }

        if (key == null) {
            LOG.warn("Unknown host for url: {}, using URL string as key", url);
            key = url.toExternalForm();
        }

        return key.toLowerCase(Locale.ROOT);
    }
}
