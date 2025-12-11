package jl95.net.io.managed;

import jl95.net.io.CloseableIos;
import jl95.net.rpc.Util;

import java.net.InetSocketAddress;

public class SimpleRetriableServerIos extends SimpleRetriableIos {

    public static SimpleRetriableServerIos of(InetSocketAddress peerAddress) {
        return new SimpleRetriableServerIos(peerAddress);
    }

    @Override protected CloseableIos connect(InetSocketAddress addr) {
        return Util.getIoAsServer(addr);
    }

    private SimpleRetriableServerIos(InetSocketAddress peerAddress) {

        super(peerAddress);
    }
}
