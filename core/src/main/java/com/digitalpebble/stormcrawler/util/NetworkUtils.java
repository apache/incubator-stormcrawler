package com.digitalpebble.stormcrawler.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

public class NetworkUtils {

    /**
     * Get all IPv4 addresses matching the `http.interface.id` config parameter
     * 
     * @param identifiers
     *            List of string to filter
     * @return list of all IPv4 addresses
     * @throws RuntimeException
     *             if no ip found
     */
    public static List<InetAddress> getIps(List<String> identifiers) {
        List<InetAddress> inetAddresses = new ArrayList<>();

        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
                 en.hasMoreElements(); ) {

                NetworkInterface networkInterface = en.nextElement();

                Optional<String> found = identifiers.stream()
                    .filter(identifier -> networkInterface.getName().startsWith(identifier))
                    .findAny();

                if (!found.isPresent()) {
                    continue;
                }

                for (Enumeration<InetAddress> enumIpAddr = networkInterface.getInetAddresses();
                     enumIpAddr.hasMoreElements(); ) {

                    InetAddress inetAddress = enumIpAddr.nextElement();
                    if (inetAddress instanceof Inet4Address) {
                        inetAddresses.add(inetAddress);
                    }
                }

            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        return inetAddresses;
    }
}
