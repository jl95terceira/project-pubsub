package jl95.net.io.managed;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;

import jl95.net.io.CloseableIos;
import jl95.net.rpc.util.Util;

public class SwitchingRetriableClientIos extends SwitchingRetriableIos {

    public static SwitchingRetriableClientIos of(Iterable<InetSocketAddress> peerAddresses) {
        return new SwitchingRetriableClientIos(peerAddresses);
    }
    public static SwitchingRetriableClientIos of(InetSocketAddress...        peerAddresses) {
        return new SwitchingRetriableClientIos(I(peerAddresses));
    }

    @Override protected CloseableIos loadIos(InetSocketAddress addr) {
        return Util.getIoAsClient(addr);
    }

    private SwitchingRetriableClientIos(Iterable<InetSocketAddress> peerAddresses) {

        super(peerAddresses);
    }
}
