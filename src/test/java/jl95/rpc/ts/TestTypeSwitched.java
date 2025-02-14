package jl95.rpc.ts;

import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.sleep;

import java.util.concurrent.CompletableFuture;

import jl95.net.rpc.ts.TypeSwitchedResponder;
import jl95.net.rpc.ts.TypeSwitchedResponderIf;
import jl95.net.rpc.ts.TypedRequester;
import jl95.net.rpc.ts.TypedRequesterIf;
import jl95.net.sr.CloseableIos;
import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterAdaptersCollection;
import jl95.net.rpc.ResponderAdaptersCollection;
import jl95.net.rpc.util.Util;

public class TestTypeSwitched {

    CloseableIos ioAsServer;
    CloseableIos ioAsClient;
    TypedRequesterIf<String, String> requester;
    TypeSwitchedResponderIf<String, String> responder;

    @org.junit.Before
    public void setUp() throws Exception {
        var requesterFuture = CompletableFuture.supplyAsync(() -> {
            ioAsServer = Util.getIoAsServer(jl95.net.sr.util.Defaults.serverAddr);
            return RequesterAdaptersCollection.asTypedStringRequester(TypedRequester.fromSimpleRpc(Requester.fromIo(ioAsServer)));
        }, (task) -> new Thread(task).start());
        sleep(50);
        var responderFuture = CompletableFuture.supplyAsync(() -> {
            ioAsClient = Util.getIoAsClient(jl95.net.sr.util.Defaults.serverAddr);
            return ResponderAdaptersCollection.asTsStringResponder(TypeSwitchedResponder.fromIo(ioAsClient));
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
        responder.adapted(Integer::parseInt, Object::toString).addCase("answer" , i -> i.equals(42));
        responder.start().await();
        org.junit.Assert.assertEquals("hello, world", requester.getFunction("hello").apply("world"));
        var intRequester = requester.adapted((Integer i) -> i.toString(), Boolean::parseBoolean);
        org.junit.Assert.assertEquals(Boolean.FALSE , intRequester.getFunction("answer").apply(100));
        org.junit.Assert.assertEquals(Boolean.TRUE  , intRequester.getFunction("answer").apply(42));
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
