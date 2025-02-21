package jl95.net.pubsub;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import jl95.net.io.util.Util;
import jl95.net.pubsub.collections.MemberAdaptersCollection;
import jl95.net.pubsub.util.Defaults;

public class Test2Clients {

    public Instant t0;
    public Broker server;
    public Member client;
    public ProducerIf<String> producer;
    public ConsumerIf<String> consumer;
    public Member client2;
    public ProducerIf<String> producer2;
    public ConsumerIf<String> consumer2;

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
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
    public void testConnectAndClose() {
    }
    @org.junit.Test
    public void testConnectAndCloseDuration() {
        var dtSetup = Duration.between(t0, Instant.now());
        System.out.printf("Setup time = %s\n", dtSetup);
        org.junit.Assert.assertTrue(dtSetup.getSeconds() < 1);
    }
    @org.junit.Test
    public void testPubSub() {
        var msgFuture  = new CompletableFuture<String>();
        consumer.consume((topic, payload) -> {
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
        consumer.consume((topic, payload) -> {
            msgFuture.complete(payload);
        });
        producer.produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertFalse(msgFuture.isDone());
    }
    @org.junit.Test
    public void testPubNoSubThenSub() {
        var msgFuture  = new CompletableFuture<String>();
        consumer.consume((topic, payload) -> {
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
