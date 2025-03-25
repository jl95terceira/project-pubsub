package jl95.net.io.managed.util;

import static jl95.lang.SuperPowers.*;

import jl95.lang.variadic.*;

public class Defaults {

    public static final Function0<Integer> reswitchTimeoutMs  = constant( 250);
    public static final Function0<Integer> reconnectTimeoutMs = constant(2000);
    public static final Function0<Integer> retryTimeoutMs     = constant( 250);

}
