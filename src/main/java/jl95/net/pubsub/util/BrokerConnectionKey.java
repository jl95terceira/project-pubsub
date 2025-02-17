package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.*;

public class BrokerConnectionKey extends NamedDataClass {

    public final InetSocketAddress inetSocketAddr;

    public BrokerConnectionKey(InetSocketAddress addr) {
        inetSocketAddr = addr;
    }
    public BrokerConnectionKey(Socket            socket) {
        this(new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }

    @Override protected Iterable<Tuple2<String, ?>> namedData() {
        return I(tuple("addr", inetSocketAddr));
    }
}
