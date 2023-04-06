package com.digitalpebble.stormcrawler.filtering.basic;

import java.util.Locale;

public class ControlCharacterEscaper implements CharacterEscaper {
    @Override
    public String escape(byte b) {
        StringBuilder sb = new StringBuilder(3);
        sb.append('%');

        String hex = Integer.toHexString(b & 0xFF).toUpperCase(Locale.ROOT);

        if (hex.length() % 2 != 0) {
            sb.append('0');
        }

        sb.append(hex);
        return sb.toString();
    }
}

