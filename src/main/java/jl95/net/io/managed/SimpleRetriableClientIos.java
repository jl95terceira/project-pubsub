package jl95.net.io.managed;

import jl95.net.io.CloseableIos;
import jl95.net.rpc.util.Util;

import java.net.InetSocketAddress;

import static jl95.lang.SuperPowers.I;

public class SimpleRetriableClientIos extends SimpleRetriableIos {

    public static SimpleRetriableClientIos of(InetSocketAddress peerAddress) {
        return new SimpleRetriableClientIos(peerAddress);
    }

    @Override protected CloseableIos connect(InetSocketAddress addr) {
        return Util.getIoAsClient(addr);
    }

    private SimpleRetriableClientIos(InetSocketAddress peerAddress) {

        super(peerAddress);
    }
}
