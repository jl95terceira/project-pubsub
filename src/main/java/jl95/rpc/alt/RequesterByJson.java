package jl95.rpc.alt;

import javax.json.JsonValue;

import jl95.net.Ios;
import jl95.rpc.Requester;
import jl95.rpc.util.SerdesDefaults;

public abstract class RequesterByJson<A, R> extends Requester<A, R> {

    public RequesterByJson(Ios io) {super(io);}

    protected abstract JsonValue writeRequestJson(A         object);
    protected abstract R         readResponseJson(JsonValue serial);

    @Override protected final byte[] writeRequest(A      object) {
        return SerdesDefaults.jsonToBytes.apply(writeRequestJson(object));
    }
    @Override protected final R      readResponse(byte[] serial) {
        return readResponseJson(SerdesDefaults.jsonFromBytes.apply(serial));
    }
}
