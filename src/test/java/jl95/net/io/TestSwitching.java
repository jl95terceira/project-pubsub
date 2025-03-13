package jl95.net.io;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jl95.lang.*;
import jl95.net.io.managed.SwitchingRetriableClientIos;
import jl95.net.io.util.Util;

public class TestSwitching {

    public static InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 42421);
    public static InetSocketAddress addr2 = new InetSocketAddress("127.0.0.1", 42422);
    public static InetSocketAddress addr3 = new InetSocketAddress("127.0.0.1", 42423);

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

    public Sender sender;

    private void assertReceivesPayload(byte[] payload, Receiver receiver) throws Exception {
        System.out.println("Payload to test: "+repr(payload));
        var payloadBackPromise = new CompletableFuture<byte[]>();
        receiver.ensureStopped();
        receiver.recv(payloadBackPromise::complete);
        sender.send(payload);
        var payloadBack = payloadBackPromise.get(2000L, TimeUnit.MILLISECONDS);
        try {
            org.junit.Assert.assertArrayEquals(payload, payloadBack);
        }
        catch (AssertionError ex) {
            System.out.println("Expected : "+repr(payload));
            System.out.println("Got      : "+repr(payloadBack));
            throw ex;
        }
    }

    @org.junit.Test
    public void testRoundRobin() throws Exception {
        var receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var receiverSocket2Future = Util.getSocketByAcceptFuture(addr2);
        var receiverSocket3Future = Util.getSocketByAcceptFuture(addr3);
        var switchingIos = SwitchingRetriableClientIos.of(addr1, addr2, addr3);
        sleep(2000);
        sender = Sender.of(switchingIos);
        System.out.println("Receiver 1 create");
        var receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream());
        System.out.println("Receiver 2 create");
        var receiver2 = Receiver.of(receiverSocket2Future.await().getInputStream()); // start 2nd receiver - sender with switching IO expected to fail-over
        System.out.println("Receiver 3 create");
        var receiver3 = Receiver.of(receiverSocket3Future.await().getInputStream()); // start 3rd receiver - sender with switching IO expected to fail-over
        System.out.println("All receivers created");
        // test 1st receiver
        assertReceivesPayload(new byte[]{0,16,0,48,0,80,0,112,0,(byte)144,0},
            receiver1);
        // test switch to 2nd receiver
        switchingIos.switchh();
        assertReceivesPayload(new byte[]{0,16,32,48,64,80,96,112,(byte)128,(byte)144,(byte)160},
            receiver2);
        // test switch to 3rd receiver
        switchingIos.switchh();
        assertReceivesPayload(new byte[]{(byte)255,16,112,96,64,80,96,112,(byte)128,(byte)144,(byte)160},
            receiver3);
        // test switch back to 1st receiver
        switchingIos.switchh();
        assertReceivesPayload(new byte[]{(byte)255,0,(byte)255,96,64,80,96,112,(byte)255,(byte)255,(byte)32},
            receiver1);
        // done
        for (var receiver: I(receiver1, receiver2, receiver3)) {
            receiver.ensureStopped();
            receiver.getInputStream().close();
        }
        switchingIos.closeAll();
    }
    @org.junit.Test
    public void testFailOver() throws Exception {
        var receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var receiverSocket2Future = Util.getSocketByAcceptFuture(addr2);
        var receiverSocket3Future = Util.getSocketByAcceptFuture(addr3);
        var switchingIos = SwitchingRetriableClientIos.of(addr1, addr2, addr3);
        sleep(2000);
        sender = Sender.of(switchingIos);
        // test 1st receiver
        var receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream());
        System.out.println("Connected to receiver 1");
        assertReceivesPayload(new byte[1000], receiver1);
        receiver1.recvStop();
        receiver1.getInputStream().close();
        // test fail-over to 2nd receiver
        var payload = new byte[]{0,16,32,48,64,80,96,112,(byte)128,(byte)144,(byte)160};
        sender.send(payload); // will fail to send to 1st receiver
        var receiver2 = Receiver.of(receiverSocket2Future.await().getInputStream()); // start 2nd receiver - sender with switching IO expected to fail-over
        System.out.println("Switched (fail-over) to receiver 2");
        assertReceivesPayload(payload, receiver2);
        receiver2.getInputStream().close();
        // test fail-over to 3rd receiver
        var payload2 = new byte[]{(byte)255,16,112,96,64,80,96,112,(byte)128,(byte)144,(byte)160};
        sender.send(payload2); // will fail to send to 2nd receiver
        var receiver3 = Receiver.of(receiverSocket3Future.await().getInputStream()); // start 3rd receiver - sender with switching IO expected to fail-over
        System.out.println("Switched (fail-over) to receiver 3");
        assertReceivesPayload(payload2, receiver3);
        receiver3.getInputStream().close();
        // test fail-over to 1st receiver (re-opened)
        receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var payload3 = new byte[]{(byte)255,0,(byte)255,96,64,80,96,112,(byte)255,(byte)255,(byte)32};
        sender.send(payload3); // will fail to send to 2nd receiver
        receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream()); // re-launch 1st receiver
        System.out.println("Switched (fail-over) to receiver 1");
        assertReceivesPayload(payload3, receiver1);
        receiver1.getInputStream().close();
        for (var receiver: I(receiver1, receiver2, receiver3)) {
            receiver.ensureStopped();
            receiver.getInputStream().close();
        }
        // done
        switchingIos.closeAll();
    }
}
