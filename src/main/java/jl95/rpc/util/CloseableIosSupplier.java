package jl95.rpc.util;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Method1;
import jl95.net.IosSupplier;

public interface CloseableIosSupplier extends IosSupplier {

    public void close();

    static CloseableIosSupplier of(InputStream  is,
                                   OutputStream os) { return of(() -> is, () -> os); }
    static CloseableIosSupplier of(Function0<InputStream>  isSupplier,
                                   Function0<OutputStream> osSupplier) {
        return of(isSupplier, osSupplier, (self) -> {
                try { self.getInputStream ().close(); } catch (Exception ex) {}
                try { self.getOutputStream().close(); } catch (Exception ex) {}
        });
    }
    static CloseableIosSupplier of(Function0<InputStream>  isSupplier,
                                   Function0<OutputStream> osSupplier,
                                   Method1<CloseableIosSupplier> closer) {
        return new CloseableIosSupplier() {
            @Override public InputStream  getInputStream () {
                return isSupplier.apply();
            }
            @Override public OutputStream getOutputStream() {
                return osSupplier.apply();
            }
            @Override public void         close          () { closer.accept(this); }
        };
    }
    static CloseableIosSupplier of(Socket socket) {

        return CloseableIosSupplier.of(unchecked(socket::getInputStream), unchecked(socket::getOutputStream), self -> {
            uncheck(socket::close);
        });
    }
}
