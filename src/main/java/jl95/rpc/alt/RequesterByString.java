package jl95.rpc.alt;

import jl95.net.Io;
import jl95.rpc.Requester;
import jl95.rpc.util.SerdesDefaults;

public abstract class RequesterByString<A, R> extends Requester<A, R> {

    public RequesterByString(Io io) {super(io);}

    protected abstract String writeRequestString(A      object);
    protected abstract R      readResponseString(String serial);

    @Override protected final byte[] writeRequest(A      object) {
        return SerdesDefaults.stringToBytes.apply(writeRequestString(object));
    }
    @Override protected final R      readResponse(byte[] serial) {
        return readResponseString(SerdesDefaults.stringFromBytes.apply(serial));
    }
}
