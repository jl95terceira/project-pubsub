package jl95.net;

import java.io.InputStream;

import jl95.lang.variadic.*;
import jl95.pubsub.util.SerdesDefaults;

public class StringReceiver extends Receiver<String> {

    public StringReceiver(InputStream in) {
        super(in);
    }

    @Override protected String fromBytes(byte[] incoming)  { return SerdesDefaults.stringFromBytes.call(incoming); }
}
