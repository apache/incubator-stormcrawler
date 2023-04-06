package com.digitalpebble.stormcrawler.filtering.basic;

public class NonControlCharacterEscaper implements CharacterEscaper {
    @Override
    public String escape(byte b) {
        return String.valueOf((char) b);
    }
}
