package jl95.rpc.util;

import static jl95.lang.SuperPowers.*;

import jl95.lang.*;
import jl95.lang.variadic.*;

public class TypedPayload extends NamedDataClass {

    public String typeAlias;
    public byte[] payload;

    public TypedPayload(String typeAlias, byte[] object) {
        this.typeAlias = typeAlias;
        this.payload = object;
    }
    public TypedPayload() {this(null, null);}

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("typeAlias", typeAlias),
            tuple("object", payload)
        );
    }
}
