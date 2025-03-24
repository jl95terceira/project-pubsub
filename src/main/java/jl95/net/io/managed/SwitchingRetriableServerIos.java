package jl95.net.io.managed;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;

import jl95.net.io.CloseableIos;
import jl95.net.rpc.util.Util;

public class SwitchingRetriableServerIos extends SwitchingRetriableIos {

    public static SwitchingRetriableServerIos of(Iterable<InetSocketAddress> peerAddresses) {
        return new SwitchingRetriableServerIos(peerAddresses);
    }
    public static SwitchingRetriableServerIos of(InetSocketAddress...        peerAddresses) {
        return new SwitchingRetriableServerIos(I(peerAddresses));
    }

    @Override protected CloseableIos connect(InetSocketAddress addr) {
        return Util.getIoAsServer(addr);
    }

    private SwitchingRetriableServerIos(Iterable<InetSocketAddress> peerAddresses) {

        super(peerAddresses);
    }
}
