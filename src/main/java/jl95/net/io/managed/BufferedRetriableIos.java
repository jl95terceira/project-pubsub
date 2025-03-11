package jl95.net.io.managed;

import static jl95.lang.SuperPowers.ifNull;
import static jl95.lang.SuperPowers.sleep;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jl95.lang.variadic.*;
import jl95.net.io.Ios;

public class BufferedRetriableIos implements ManagedIos {

    public static class NoMoreRetriesException extends RuntimeException {}

    private final Function0<Ios>              iosSupplier;
    private       Ios                         ios;
    private       Integer                     retryTimeoutMs;
    private       Function1<Boolean, Integer> retryPredicate;

    private void  reloadIos() {
        ios = iosSupplier.apply();
    }
    private <T> T retried(Function1<T, Ios> f) {
        var retriesSoFar = 0;
        while (true) {
            try {
                if (ios == null) {
                    reloadIos();
                }
                try {
                    return f.apply(ios);
                }
                catch (Exception ex) {
                    reloadIos();
                    throw ex;
                }
            }
            catch (Exception ex) {
                if (!ifNull(retryPredicate, n -> true).apply(retriesSoFar)) {
                    throw new NoMoreRetriesException();
                }
                retriesSoFar += 1;
                sleep(ifNull(retryTimeoutMs, 250));
            }
        }
    }

    public BufferedRetriableIos(Function0<Ios> iosSupplier) {

        this.iosSupplier = iosSupplier;
    }

    public final void  setRetryTimeoutMs(Integer t) { this.retryTimeoutMs = t; }
    public final void  setRetryPredicate(Function1<Boolean, Integer> f) { this.retryPredicate = f; }
    public final void  setRetryLimit    (Integer max) { setRetryPredicate(n -> n <= max); }

    public final <T> T withInput (Function1<T, InputStream>  f) {
        return retried((ios) -> f.apply(ios.getInputStream ()));
    }
    public final <T> T withOutput(Function1<T, OutputStream> f) {
        return retried((ios) -> f.apply(ios.getOutputStream()));
    }
    public final <T> T withIo    (Function2<T, InputStream, OutputStream> f) { return retried((ios) -> f.apply(ios.getInputStream(), ios.getOutputStream())); }
}
