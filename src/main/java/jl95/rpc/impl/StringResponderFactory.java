package jl95.rpc.impl;

import jl95.net.Io;
import jl95.rpc.ResponderIf;
import jl95.rpc.util.SerdesDefaults;

public class StringResponderFactory {

    public static ResponderIf<String, String> get(Io io) {

        return new BytesResponderFactory(io).adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }

    private StringResponderFactory() {}
}
