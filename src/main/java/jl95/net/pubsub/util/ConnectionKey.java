package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.*;

public class ConnectionKey extends NamedDataClass {

    public final InetSocketAddress inetSocketAddr;

    public ConnectionKey(InetSocketAddress addr) {
        inetSocketAddr = addr;
    }
    public ConnectionKey(Socket            socket) {
        this(new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }

    @Override protected Iterable<Tuple2<String, ?>> namedData() {
        return I(tuple("addr", inetSocketAddr));
    }
}
