package jl95.net;

import static java.lang.String.*;

import java.net.ServerSocket;
import java.util.*;

import jl95.net.util.ReceiverBySocket;
import jl95.net.util.SenderBySocket;

public class Test {

    private static java.net.InetSocketAddress addr = new java.net.InetSocketAddress("127.0.0.1", 42422);
    private static Boolean toStop = false;

    static { Runtime.getRuntime().addShutdownHook(new Thread(() -> { toStop = true; })); }

    // toy main
    public static void main(String[] args) throws Exception {

        System.out.printf("Address: %s\n", addr);
        var serversock = new ServerSocket();
        serversock.bind(addr);
        new Thread(() -> {
            try {
                var sock = serversock.accept();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        sock.close();
                    }
                    catch(Exception ex) {}
                }));
                ReceiverBySocket.get(sock, StringReceiver::new).recv(System.out::println);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).start();
        var client = new java.net.Socket();
        client.connect(addr);
        while (!toStop) {
             SenderBySocket.get(client, StringSender::new).send(format("Hello, at %s;", java.time.Instant.now()));
             Thread.sleep(1000);
        }
        System.out.println("Done");
    }

    @org.junit.Test public void test() throws Exception {

        List<String> messagesSend = new ArrayList<>(100);
        for (int i = 0; i < 1000; i++) {
            messagesSend.add(UUID.randomUUID().toString().repeat(1000));
        }
        System.out.printf("Testing send-receive (through localhost) for %s messages\n", messagesSend.size());
        var serversock = new ServerSocket();
        serversock.bind(addr);
        int[] charsReceivedNr = { 0 };
        new Thread(() -> {
            try {
                var sock = serversock.accept();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        sock.close();
                    }
                    catch(Exception ex) {}
                }));
                var messagesSendIterator = messagesSend.iterator();
                ReceiverBySocket.get(sock, StringReceiver::new).recv(message -> {
                    charsReceivedNr[0] += message.length();
                    org.junit.Assert.assertTrue  (messagesSendIterator.hasNext());
                    org.junit.Assert.assertEquals(messagesSendIterator.next(), message);
                });
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).start();
        var client = new java.net.Socket();
        client.connect(addr);
        for (var message: messagesSend) {
             SenderBySocket.get(client, StringSender::new).send(message);
        }
        System.out.println("Exchanged a total of "+charsReceivedNr[0]+" characters");
        System.out.println("OK!");
    }
}
