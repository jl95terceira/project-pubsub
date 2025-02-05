package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

import javax.json.JsonValue;

import jl95.rpc.util.SerdesDefaults;

public class JsonRequester {

    public static Requester<JsonValue, JsonValue> get(OutputStream      output,
                                                      InputStream       input,
                                                      GenericRequester.Options options) {

        return StringRequester.get(output, input, options).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
}
