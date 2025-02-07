package jl95.rpc.impl;

import jl95.net.Io;
import jl95.rpc.Requester;
import jl95.rpc.RequesterIf;
import jl95.rpc.util.SerdesDefaults;

public class StringRequesterFactory {

    public static RequesterIf<String, String> get(Io io, Requester.SendOptions options) {

        return BytesRequesterFactory.get(io, options).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
}
