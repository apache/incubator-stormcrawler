package com.digitalpebble.stormcrawler.persistence.urlbuffer;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.InitialisationUtil;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public final class URLBufferUtil {
    private URLBufferUtil() {}

    /** Implementation to use for URLBuffer. Must implement the interface URLBuffer. */
    public static final String bufferClassParamName = "urlbuffer.class";

    /** Returns a URLBuffer instance based on the configuration * */
    public static URLBuffer createInstance(Map<String, Object> stormConf) {
        URLBuffer buffer;

        String className = ConfUtils.getString(stormConf, bufferClassParamName);

        if (StringUtils.isBlank(className)) {
            throw new RuntimeException("Missing value for config  " + bufferClassParamName);
        }

        try {
            buffer = InitialisationUtil.initializeFromQualifiedName(className, URLBuffer.class);
            buffer.configure(stormConf);
        } catch (Exception e) {
            throw new RuntimeException("Can't instanciate " + className, e);
        }

        return buffer;
    }
}
