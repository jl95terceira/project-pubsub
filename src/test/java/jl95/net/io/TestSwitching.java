package jl95.net.io;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jl95.lang.*;
import jl95.net.io.managed.SwitchingRetriableClientIos;
import jl95.net.io.managed.SwitchingRetriableIos;
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

    public Receiver receiver1;
    public Receiver receiver2;
    public Receiver receiver3;
    public Sender sender;
    public SwitchingRetriableIos switchingIos;

    private void assertReceivesPayload        (byte[] payload, Receiver receiver) throws Exception {
        System.out.println("Payload to test: "+repr(payload));
        var payloadBackPromise = new CompletableFuture<byte[]>();
        receiver.ensureStopped();
        receiver.recv(payloadBackPromise::complete).await();
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
    private void assertReceivesPayloadAndClose(byte[] payload, Receiver receiver) throws Exception {
        assertReceivesPayload(payload, receiver);
        receiver.ensureStopped();
        receiver.getInputStream().close();
    }

    @org.junit.After
    public void tearDown() throws Exception {
        for (var receiver: I(receiver1, receiver2, receiver3)) {
            if (receiver != null)
            {
                receiver.ensureStopped();
                receiver.getInputStream().close();
            }
        }
        if (switchingIos != null) {
            switchingIos.close();
        }
    }
    @org.junit.AfterClass
    public static void tearDownStatic() {
        sleep(1000);
    }

    @org.junit.Test
    public void testRoundRobin() throws Exception {
        var receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var receiverSocket2Future = Util.getSocketByAcceptFuture(addr2);
        var receiverSocket3Future = Util.getSocketByAcceptFuture(addr3);
        switchingIos = SwitchingRetriableClientIos.of(addr1, addr2, addr3);
        switchingIos.setReswitchHandler((addr_prev,addr_new) -> {
            System.out.printf("Switching from %s to %s\n", addr_prev, addr_new);
        });
        sleep(1000);
        System.out.println("Sender create");
        sender = Sender.of(switchingIos);
        System.out.println("Receiver 1 create");
        receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream());
        System.out.println("Receiver 2 create");
        receiver2 = Receiver.of(receiverSocket2Future.await().getInputStream()); // start 2nd receiver - sender with switching IO expected to fail-over
        System.out.println("Receiver 3 create");
        receiver3 = Receiver.of(receiverSocket3Future.await().getInputStream()); // start 3rd receiver - sender with switching IO expected to fail-over
        System.out.println("All receivers created");
        // test 1st receiver
        assertReceivesPayload(new byte[]{0,16,0,48,0,80,0,112,0,(byte)144,0},
            receiver1);
        // test switch to 2nd receiver
        switchingIos.switchAddress();
        assertReceivesPayload(new byte[]{0,16,32,48,64,80,96,112,(byte)128,(byte)144,(byte)160},
            receiver2);
        // test switch to 3rd receiver
        switchingIos.switchAddress();
        assertReceivesPayload(new byte[]{(byte)255,16,112,96,64,80,96,112,(byte)128,(byte)144,(byte)160},
            receiver3);
        // test switch back to 1st receiver
        switchingIos.switchAddress();
        assertReceivesPayload(new byte[]{(byte)255,0,(byte)255,96,64,80,96,112,(byte)255,(byte)255,(byte)32},
            receiver1);
        // test switch back to 3rd receiver
        switchingIos.switchAddress(); // 1 -> 2
        switchingIos.switchAddress(); // 2 -> 3
        assertReceivesPayload(new byte[200],
            receiver3);
    }
    @org.junit.Test
    public void testFailOver() throws Exception {
        var receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var receiverSocket2Future = Util.getSocketByAcceptFuture(addr2);
        var receiverSocket3Future = Util.getSocketByAcceptFuture(addr3);
        switchingIos = SwitchingRetriableClientIos.of(addr1, addr2, addr3);
        switchingIos.setReswitchHandler((addr_prev,addr_new) -> {
            System.out.printf("Switching from %s to %s\n", addr_prev, addr_new);
        });
        sleep(1000);
        sender = Sender.of(switchingIos);
        var payload1 = new byte[1000];
        receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream());
        System.out.println("Connected to receiver 1");
        assertReceivesPayloadAndClose(payload1, receiver1);
        // test fail-over to 2nd receiver
        var payload2 = new byte[]{0,16,32,48,64,80,96,112,(byte)128,(byte)144,(byte)160};
        receiver2 = Receiver.of(receiverSocket2Future.await().getInputStream()); // start 2nd receiver - sender with switching IO expected to fail-over
        System.out.println("Switch (fail-over) to receiver 2");
        assertReceivesPayloadAndClose(payload2, receiver2);
        // test fail-over to 3rd receiver
        var payload3 = new byte[]{(byte)255,16,112,96,64,80,96,112,(byte)128,(byte)144,(byte)160};
        receiver3 = Receiver.of(receiverSocket3Future.await().getInputStream()); // start 3rd receiver - sender with switching IO expected to fail-over
        System.out.println("Switch (fail-over) to receiver 3");
        assertReceivesPayloadAndClose(payload3, receiver3);
        // test fail-over to 1st receiver (re-opened)
        var payload4 = new byte[]{(byte)255,0,(byte)255,96,64,80,96,112,(byte)255,(byte)255,(byte)32};
        receiver1 = Receiver.of(Util.getSocketByAcceptFuture(addr1).await().getInputStream()); // re-launch 1st receiver
        System.out.println("Switch (fail-over) back to receiver 1");
        assertReceivesPayloadAndClose(payload4, receiver1);
        // test fail-over to 3rd receiver (re-opened)
        var payload5 = new byte[]{(byte)255,0,(byte)255,16,64,80,16,112,(byte)255,(byte)255,(byte)32};
        receiver3 = Receiver.of(Util.getSocketByAcceptFuture(addr3).await().getInputStream()); // re-launch 1st receiver
        System.out.println("Switch (fail-over) back to receiver 3");
        assertReceivesPayloadAndClose(payload5, receiver3);
    }
}
