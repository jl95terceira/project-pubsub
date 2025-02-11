package jl95.net;

import static java.lang.String.*;
import static jl95.lang.SuperPowers.uncheck;

import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Test {

    private static java.net.InetSocketAddress addr = new java.net.InetSocketAddress("127.0.0.1", 42422);
    private static Boolean toStop = false;

    static { Runtime.getRuntime().addShutdownHook(new Thread(() -> { toStop = true; })); }

    private Receiver<String> receiver;
    private Sender  <String> sender;

    @org.junit.Before
    public void setUp() throws Exception {
        var serversock = new ServerSocket();
        serversock.bind(addr);
        CompletableFuture<Receiver<String>> receiverFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                var sock = serversock.accept();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        sock.close();
                    }
                    catch(Exception ex) {}
                }));
                serversock.close();
                receiverFuture.complete(ReceiversCollection.getStringReceiver(uncheck(sock::getInputStream)));
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).start();
        var client = new java.net.Socket();
        client.connect(addr);
        sender   = SendersCollection.getStringSender(uncheck(client::getOutputStream));
        receiver = receiverFuture.get();
    }
    @org.junit.After
    public void tearDown() throws Exception {
        sender  .getOutputStream().close();
        receiver.getInputStream ().close();
    }

    @org.junit.Test public void testStartStop() {

        org.junit.Assert.assertFalse(receiver.isReceiving());
        receiver.recv(x -> {}).await();
        org.junit.Assert.assertTrue(receiver.isReceiving());
        receiver.recvStop().await();
        org.junit.Assert.assertFalse(receiver.isReceiving());
    }
    @org.junit.Test public void test() {

        List<String> messagesSend = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            messagesSend.add(UUID.randomUUID().toString().repeat(10));
        }
        System.out.printf("Testing send-receive (through localhost) for %s messages\n", messagesSend.size());
        int[] charsReceivedNr = { 0 };
        var messagesSendIterator = messagesSend.iterator();
        receiver.recv(message -> {
            charsReceivedNr[0] += message.length();
            org.junit.Assert.assertTrue  (messagesSendIterator.hasNext());
            org.junit.Assert.assertEquals(messagesSendIterator.next(), message);
        }).await();
        for (var message: messagesSend) {
             sender.send(message);
        }
        System.out.println("Exchanged a total of "+charsReceivedNr[0]+" characters");
        receiver.recvStop().await();
    }
    @org.junit.Test public void testException() {
        receiver.recv(x -> { throw new RuntimeException(); }).await();
        sender.send("abc");
        receiver.recvStop().await();
    }
}
