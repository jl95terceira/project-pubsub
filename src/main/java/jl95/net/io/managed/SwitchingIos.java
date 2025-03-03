package jl95.net.io.managed;

import static jl95.lang.SuperPowers.*;
import static jl95.net.io.util.Util.getSocketByConnect;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import jl95.lang.I;
import jl95.lang.Ref;
import jl95.lang.variadic.Function0;
import jl95.net.io.Ios;

public class SwitchingIos extends BufferedRetriableIos {

    private static Function0<Ios> getIosProxy(Iterable<InetSocketAddress> addresses) {
        var addressesList = I.of(addresses).cycle();
        var addressesIterator = new Ref<>(addressesList.iterator());
        if (!addressesIterator.value.hasNext()) {
            throw new IllegalArgumentException("addresses list must not be empty");
        }
        return () -> Ios.fromSocketLazy(getSocketByConnect(addressesIterator.value.next()));
    }

    private final Function0<Ios> iosSupplier;

    private SwitchingIos(Function0<Ios> iosSupplier) {
        super(iosSupplier);
        this.iosSupplier = iosSupplier;
    }

    public SwitchingIos(InetSocketAddress... addresses) {
        this(I(addresses));
    }
    public SwitchingIos(Iterable<InetSocketAddress> addresses) { this(getIosProxy(addresses)); }
}
