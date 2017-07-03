package com.digitalpebble.storm.crawler.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetworkUtils {
    /**
     * Get all IPv4 addresses
     *
     * @param config Storm the
     * @return list of all IPv4 addresses of the host
     * @throws SocketException
     */
    public static List<InetAddress> getIps(Config config) throws SocketException {
        List<String> identifiers = new ArrayList<>();
        Object obj = config.get("http.interface.id");

        if (obj == null) {
            identifiers.add("eth0");
        } else {
            if (obj instanceof List) {
                identifiers.addAll((List) obj);
            } else {
                identifiers.add(obj.toString());
            }
        }

        List<InetAddress> inetAddresses = new ArrayList<>();

        for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            NetworkInterface intf = en.nextElement();
            Optional<String> found = identifiers.stream()
                .filter(identifier -> intf.getName().startsWith(identifier))
                .findAny();
            if (!found.isPresent()) {
                continue;
            }

            for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
                InetAddress iadd = enumIpAddr.nextElement();
                if (iadd instanceof Inet4Address) {
                    inetAddresses.add(iadd);
                }
            }

        }
        return inetAddresses;
    }
}
