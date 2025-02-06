package jl95.rpc;

import jl95.net.Io;
import jl95.rpc.util.SerdesDefaults;

public class StringRequesterFactory {

    public static RequesterIf<String, String> get(Io io, Requester.SendOptions options) {

        return BytesRequesterFactory.get(io, options).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
}
