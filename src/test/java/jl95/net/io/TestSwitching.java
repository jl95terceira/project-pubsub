package jl95.net.io;

import java.net.InetSocketAddress;

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
//        var sender = Sender.of(InputStreams.getLazy(new SwitchingIos(addr1, addr2)));
//        var receiver1 = receiverSocket1Future.await();
//        var receiver2 = receiverSocket2Future.await();
//        sender.close();
//        receiver1.close();
//        receiver2.close();
    }
}
