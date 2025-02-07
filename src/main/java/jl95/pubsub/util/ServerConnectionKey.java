package jl95.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.*;

public class ServerConnectionKey extends NamedDataClass {

    public final InetSocketAddress inetSocketAddr;

    public ServerConnectionKey(InetSocketAddress addr) {
        inetSocketAddr = addr;
    }
    public ServerConnectionKey(Socket            socket) {
        this(new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }

    @Override protected Iterable<Tuple2<String, ?>> namedData() {
        return I(tuple("addr", inetSocketAddr));
    }
}
