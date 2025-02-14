package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.util.concurrent.CompletableFuture;

import jl95.net.pubsub.Member;
import jl95.net.pubsub.MemberAdaptersCollection;
import jl95.net.pubsub.ConsumerIf;
import jl95.net.pubsub.ProducerIf;
import jl95.net.pubsub.Broker;
import jl95.net.sr.util.Util;
import jl95.net.pubsub.util.Defaults;

public class Test {

    public Broker server;
    public Member client;
    public ProducerIf<String> producer;
    public ConsumerIf<String> consumer;
    public Member client2;
    public ProducerIf<String> producer2;
    public ConsumerIf<String> consumer2;

    @org.junit.Before
    public void setUp() {
        server = new Broker(Util.getSimpleServerSocket(Defaults.serverAddr, Defaults.serverAcceptTimeoutMs));
        server.startAccept().await();
        client  = new Member(Defaults.serverAddr);
        producer = MemberAdaptersCollection.getStringProducer(client);
        consumer = MemberAdaptersCollection.getStringConsumer(client);
        client2 = new Member(Defaults.serverAddr);
        producer2 = MemberAdaptersCollection.getStringProducer(client2);
        consumer2 = MemberAdaptersCollection.getStringConsumer(client2);
    }
    @org.junit.After
    public void tearDown() {
        client.close();
        client2.close();
        server.stopAccept().await();
        uncheck(() -> server.getNetServer().getSocket().close());
    }

    @org.junit.Test
    public void testConnectAndClose() {}
    @org.junit.Test
    public void testPubSub() {
        var msgFuture  = new CompletableFuture<String>();
        consumer.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        client.subscribeByList(I("foo"));
        sleep(125);
        producer.produce("foo", "BAR");
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
    @org.junit.Test
    public void testPubNoSub() {
        var msgFuture  = new CompletableFuture<String>();
        consumer.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        producer.produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertFalse(msgFuture.isDone());
    }
    @org.junit.Test
    public void testPubNoSubThenSub() {
        var msgFuture  = new CompletableFuture<String>();
        consumer.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        producer.produce("foo", "BAR");
        org.junit.Assert.assertFalse(msgFuture.isDone());
        client.subscribeByList(I("foo"));
        sleep(125);
        producer.produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
}
