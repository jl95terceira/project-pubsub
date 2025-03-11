package jl95.net.io;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jl95.net.io.managed.SwitchingIos;
import jl95.net.io.util.InputStreams;
import jl95.net.io.util.Util;

public class TestSwitching {

    public static InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 42421);
    public static InetSocketAddress addr2 = new InetSocketAddress("127.0.0.1", 42422);
    public static InetSocketAddress addr3 = new InetSocketAddress("127.0.0.1", 42423);

    @org.junit.Test
    public void test() throws Exception {
        var receiverSocket1Future = Util.getSocketByAcceptFuture(addr1);
        var receiverSocket2Future = Util.getSocketByAcceptFuture(addr2);
        var sender = Sender.of(new SwitchingIos(addr1, addr2));
        var receiver1 = Receiver.of(receiverSocket1Future.await().getInputStream());
        System.out.println("Connected to receiver 1");
        org.junit.Assert.assertFalse(receiverSocket2Future.isDone());
        receiver1.recv(payload -> {});
        sender.send(new byte[1000]);
        receiver1.recvStop();
        receiver1.getInputStream().close();
        var payload = UUID.randomUUID().toString().getBytes();
        sender.send(payload);
        var receiver2 = Receiver.of(receiverSocket2Future.await().getInputStream());
        var payloadBackPromise = new CompletableFuture<byte[]>();
        System.out.println("Switched to receiver 2");
        receiver2.recv(payloadBackPromise::complete);
        var payloadBack = payloadBackPromise.get(2000L, TimeUnit.MILLISECONDS);
        org.junit.Assert.assertEquals(payload, payloadBack);
        receiver2.getInputStream().close();
        sender.getOutputStream().close();
    }
}
