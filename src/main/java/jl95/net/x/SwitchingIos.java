package jl95.net.x;

import static jl95.lang.SuperPowers.*;
import static jl95.net.util.Util.getConnectedSocket;

import java.net.InetSocketAddress;

import jl95.lang.I;
import jl95.lang.Ref;
import jl95.lang.variadic.Function0;
import jl95.net.Ios;

public class SwitchingIos extends BufferedRetriableIos {

    private static Function0<Ios> getIosProxy(Iterable<InetSocketAddress> addresses) {
        var addressesList = I.of(addresses).cycle();
        var addressesIterator = new Ref<>(addressesList.iterator());
        if (!addressesIterator.value.hasNext()) {
            throw new IllegalArgumentException("addresses list must not be empty");
        }
        return () -> Ios.getLazySocketIos(getConnectedSocket(addressesIterator.value.next()));
    }

    public SwitchingIos(InetSocketAddress address) {
        this(I(address));
    }
    public SwitchingIos(Iterable<InetSocketAddress> addresses) {
        super(getIosProxy(addresses));
    }
}
