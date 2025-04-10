package jl95.net.io;

import jl95.lang.I;
import jl95.net.io.managed.RetriableIos;
import jl95.net.io.managed.SimpleRetriableClientIos;
import jl95.net.io.util.Util;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static jl95.lang.SuperPowers.*;

public class TestRetriable {

    public static InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 42421);

    private static Byte[] box(byte[] bb) {
        var bb2 = new Byte[bb.length];
        var i = 0;
        for (var b: bb) {
            bb2[i] = b;
            i = i+1;
        }
        return bb2;
    }
    private static String repr(byte[] bb) {
        return String.format("[%s]", String.join(",", I.ofArray(box(bb)).map(b -> Byte.toString(b))));
    }

    public Receiver receiver;
    public Sender   sender;
    public CompletableFuture<byte[]> payloadBackPromise;
    public RetriableIos retriableIos;
    public Integer restartsNr = 0;

    private void assertReceivesPayload(byte[] payload) throws Exception {
        System.out.println("Payload to test: "+repr(payload));
        System.out.println("Sending");
        sender.send(payload);
        System.out.println("Waiting for payload back");
        var payloadBack = payloadBackPromise.get(2000L, TimeUnit.MILLISECONDS);
        System.out.println("Got payload back - OK");
        try {
            org.junit.Assert.assertArrayEquals(payload, payloadBack);
        }
        catch (AssertionError ex) {
            System.out.println("Expected : "+repr(payload));
            System.out.println("Got      : "+repr(payloadBack));
            throw ex;
        }
    }
    private void expectPayload() {
        payloadBackPromise = new CompletableFuture<>();
        receiver.recv(payloadBackPromise::complete);
    }
    private void restartReceiver() {
        restartsNr += 1;
        System.out.printf ("Restarting receiver (nr of restart = %s)\n", restartsNr);
        System.out.println("  stop");
        receiver.ensureStopped();
        System.out.println("  close (connection closed - sender will have to reconnect)");
        uncheck(receiver.getInputStream()::close);
        System.out.println("  new");
        new Thread(() -> {
            receiver = Receiver.of(uncheck(Util.getSocketByAccept(addr)::getInputStream));
            System.out.println("Restarted receiver OK");
            expectPayload();
        }).start();
    }

    @org.junit.After
    public void tearDown() throws Exception {
        if (receiver != null)
        {
            receiver.ensureStopped();
            receiver.getInputStream().close();
        }
        if (retriableIos != null) {
            retriableIos.close();
        }
    }
    @org.junit.AfterClass
    public static void tearDownStatic() {
        sleep(1000);
    }

    @org.junit.Test
    public void test() throws Exception {
        var receiverSocketFuture = Util.getSocketByAcceptFuture(addr);
        retriableIos = SimpleRetriableClientIos.of(addr);
        sleep(1000);
        sender   = Sender.of(retriableIos);
        receiver = Receiver.of(receiverSocketFuture.await().getInputStream());
        System.out.println("Connected to receiver");
        expectPayload();
        var payloadMaker = function(() -> String.join("", I.range(10).map(i -> UUID.randomUUID().toString())).getBytes());
        assertReceivesPayload(payloadMaker.apply());
        // restart many times
        for (var r: I.range(5)) {
            restartReceiver();
            sleep(1000);
            assertReceivesPayload(payloadMaker.apply());
        }
    }
}
