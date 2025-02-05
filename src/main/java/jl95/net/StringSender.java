package jl95.net;

import java.io.OutputStream;

import jl95.lang.variadic.*;
import jl95.pubsub.util.SerdesDefaults;

public class StringSender extends Sender<String> {

    public StringSender(OutputStream out) {
        super(out);
    }

    @Override protected byte[] toBytes  (String outgoing) { return SerdesDefaults.stringToBytes.call(outgoing); }
}
