package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

import javax.json.JsonValue;

import jl95.rpc.util.SerdesDefaults;

public class JsonResponder {

    public static Responder<JsonValue, JsonValue> get(InputStream input, OutputStream output) {

        return StringResponder.get(input, output).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
}
