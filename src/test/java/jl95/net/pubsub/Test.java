package jl95.net.pubsub;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.tuple;
import static jl95.lang.SuperPowers.uncheck;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import jl95.net.io.util.Util;
import jl95.net.pubsub.collections.MemberAdaptersCollection;
import jl95.net.pubsub.util.Defaults;

public class Test {

    public static boolean TEST_MULTI_BROKER = true;

    public Instant t0;
    public final InetSocketAddress brokerAddr1 = new InetSocketAddress("127.0.0.1", 42421);
    public final InetSocketAddress brokerAddr2 = new InetSocketAddress("127.0.0.1", 42422);
    public Broker broker1;
    public Broker broker2;
    public Member member1;
    public Member member2;
    public Member member3;

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
        broker1 = new Broker(Util.getSimpleServerSocket(brokerAddr1));
        broker2 = new Broker(Util.getSimpleServerSocket(brokerAddr2));
        broker1.startAccept().await();
        broker2.startAccept().await();
        broker1.linkBroker(brokerAddr2);
        broker2.linkBroker(brokerAddr1);
        member1 = new Member(brokerAddr1);
        member2 = new Member(brokerAddr1);
        member3 = new Member(brokerAddr2);
    }
    @org.junit.After
    public void tearDown() {
        member1.close();
        member2.close();
        member3.close();
        broker1.stopAccept().await();
        broker2.stopAccept().await();
        uncheck(() -> broker1.getNetServer().getSocket().close());
        uncheck(() -> broker2.getNetServer().getSocket().close());
    }

    public void testPubSub         (Member producerMember, Member consumerMember) {
        var msgFuture = new CompletableFuture<String>();
        MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
            msgFuture.complete(payload);
        });
        consumerMember.subscribeByList(I("foo"));
        MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
    public void testPubNoSub       (Member producerMember, Member consumerMember) {
        var msgFuture = new CompletableFuture<String>();
        MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
            msgFuture.complete(payload);
        });
        MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertFalse(msgFuture.isDone());
    }
    public void testPubNoSubThenSub(Member producerMember, Member consumerMember) {
        var msgFuture = new CompletableFuture<String>();
        MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
            msgFuture.complete(payload);
        });
        sleep(125);
        MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
        org.junit.Assert.assertFalse(msgFuture.isDone());
        consumerMember.subscribeByList(I("foo"));
        MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }

    @org.junit.Test
    public void testConnectAndClose() {
    }
    @org.junit.Test
    public void testPubSubSameMember() {
        testPubSub(member1, member1);
    }
    @org.junit.Test
    public void testPubSubSameBroker() {
        testPubSub(member1, member2);
    }
    @org.junit.Test
    public void testPubSubMultiBroker() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER);
        testPubSub(member1, member3);
    }
    @org.junit.Test
    public void testPubNoSubSameMember() {
        testPubNoSub(member1, member1);
    }
    @org.junit.Test
    public void testPubNoSubSameBroker() {
        testPubNoSub(member1, member2);
    }
    @org.junit.Test
    public void testPubNoSubMultiBroker() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER);
        testPubNoSub(member1, member3);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameMember() {
        testPubNoSubThenSub(member1, member1);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameBroker() {
        testPubNoSubThenSub(member1, member2);
    }
    @org.junit.Test
    public void testPubNoSubThenSubMultiBroker() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER);
        testPubNoSubThenSub(member1, member3);
    }
}
