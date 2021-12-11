package com.digitalpebble.stormcrawler.persistence.urlbuffer;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public final class URLBufferUtil {
    private URLBufferUtil() {}

    /** Implementation to use for URLBuffer. Must implement the interface URLBuffer. */
    public static final String bufferClassParamName = "urlbuffer.class";

    /** Returns a URLBuffer instance based on the configuration * */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static URLBuffer createInstance(Map stormConf) {
        URLBuffer buffer;

        String className = ConfUtils.getString(stormConf, bufferClassParamName);

        if (StringUtils.isBlank(className)) {
            throw new RuntimeException("Missing value for config  " + bufferClassParamName);
        }

        try {
            Class<?> bufferclass = Class.forName(className);
            boolean interfaceOK = URLBuffer.class.isAssignableFrom(bufferclass);
            if (!interfaceOK) {
                throw new RuntimeException("Class " + className + " must extend URLBuffer");
            }
            buffer = (URLBuffer) bufferclass.newInstance();
            buffer.configure(stormConf);
        } catch (Exception e) {
            throw new RuntimeException("Can't instanciate " + className);
        }

        return buffer;
    }
}
