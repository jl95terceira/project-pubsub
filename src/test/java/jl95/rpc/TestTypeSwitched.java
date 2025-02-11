package jl95.rpc;

import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.sleep;

import java.util.concurrent.CompletableFuture;

import jl95.rpc.util.CloseableIosSupplier;
import jl95.rpc.util.Util;

public class TestTypeSwitched {

    CloseableIosSupplier ioAsServer;
    CloseableIosSupplier ioAsClient;
    TypedRequester       <String, String> requester;
    TypeSwitchedResponder<String, String> responder;

    @org.junit.Before
    public void setUp() throws Exception {
        var requesterFuture = CompletableFuture.supplyAsync(() -> {
            ioAsServer = Util.getIoAsServer(jl95.net.util.Defaults.serverAddr);
            return TypedRequestersCollection.getStringRequester(ioAsServer);
        }, (task) -> new Thread(task).start());
        sleep(50);
        var responderFuture = CompletableFuture.supplyAsync(() -> {
            ioAsClient = Util.getIoAsClient(jl95.net.util.Defaults.serverAddr);
            return TypeSwitchedRespondersCollection.getStringResponder(ioAsClient);
        }, (task) -> new Thread(task).start());
        requester = requesterFuture.get();
        responder = responderFuture.get();
    }
    @org.junit.After
    public void tearDown() {
        if (ioAsClient != null) ioAsClient.close();
        if (ioAsServer != null) ioAsServer.close();
        if (responder.isRunning()) responder.stop().await();
    }

    @org.junit.Test
    public void test() {
        responder.addCase("hello"  , msg -> "hello, " + msg);
        responder.addCase("bye"    , msg -> "bye, "   + msg);
        responder.addCase("answer" , i -> i.equals(42), Integer::parseInt, Object::toString);
        responder.start().await();
        org.junit.Assert.assertEquals("hello, world", requester.getFunction("hello").apply("world"));
        org.junit.Assert.assertEquals(Boolean.FALSE , requester.getFunction("answer", (Integer i) -> i.toString(), Boolean::parseBoolean).apply(100));
        org.junit.Assert.assertEquals(Boolean.TRUE  , requester.getFunction("answer", (Integer i) -> i.toString(), Boolean::parseBoolean).apply(42));
        org.junit.Assert.assertEquals("bye, world"  , requester.getFunction("bye").apply("world"));
    }
    @org.junit.Test
    public void testStartStop() {
        org.junit.Assert.assertFalse(responder.isRunning());
        responder.start().await();
        org.junit.Assert.assertTrue (responder.isRunning());
        responder.stop().await();
        org.junit.Assert.assertFalse(responder.isRunning());
        responder.start().await();
        org.junit.Assert.assertTrue (responder.isRunning());
    }
}
