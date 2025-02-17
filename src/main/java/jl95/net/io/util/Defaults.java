package jl95.net.io.util;

import java.net.InetSocketAddress;

public class Defaults {

    public static final InetSocketAddress serverAddr = new InetSocketAddress("127.0.0.1", 4242);
    public static final Integer           acceptTimeoutMs = 1000;
    public static final Integer           acceptTimeoutMsForSingleClient = 10000;
}
