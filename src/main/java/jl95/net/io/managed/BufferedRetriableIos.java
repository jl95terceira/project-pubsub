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
    private <T> T retried(ExceptFunction0<T, IOException> f) {
        var retriesSoFar = 0;
        while (true) {
            try {
                if (ios == null) {
                    reloadIos();
                }
                try {
                    return f.apply();
                }
                catch (IOException ex) {
                    reloadIos();
                    throw ex;
                }
            }
            catch (IOException ex) {
                if (!ifNull(retryPredicate, n -> true).apply(retriesSoFar)) {
                    throw new NoMoreRetriesException();
                }
                retriesSoFar += 1;
                sleep(ifNull(retryTimeoutMs, 250));
            }
        }
    }
    private void  retried(ExceptMethod0<IOException> f) {
        this.<Void>retried(() -> { f.accept(); return null; });
    }

    public BufferedRetriableIos(Function0<Ios> iosSupplier) {

        this.iosSupplier = iosSupplier;
    }

    public final void  setRetryTimeoutMs(Integer t) { this.retryTimeoutMs = t; }
    public final void  setRetryPredicate(Function1<Boolean, Integer> f) { this.retryPredicate = f; }
    public final void  setRetryLimit    (Integer max) { setRetryPredicate(n -> n <= max); }

    public final <T> T withInput (ExceptFunction1<T, IOException, InputStream> f) {
        return retried(() -> f.apply(ios.getInputStream()));
    }
    public final <T> T withOutput(ExceptFunction1<T, IOException, OutputStream> f) {
        return retried(() -> f.apply(ios.getOutputStream()));
    }
}
