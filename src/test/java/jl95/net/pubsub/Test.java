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

public class Test {

    public static boolean TEST_SAME_MEMBER  = true;
    public static boolean TEST_SAME_BROKER  = true;
    public static boolean TEST_MULTI_BROKER_ONE_JUMP  = true;
    public static boolean TEST_MULTI_BROKER_TWO_JUMPS = true;

    public Instant t0;
    public final InetSocketAddress brokerAddr1 = new InetSocketAddress("127.0.0.1", 42421);
    public final InetSocketAddress brokerAddr2 = new InetSocketAddress("127.0.0.1", 42422);
    public final InetSocketAddress brokerAddr3 = new InetSocketAddress("127.0.0.1", 42423);
    public Broker broker1;
    public Broker broker2;
    public Broker broker3;
    public Member member1;
    public Member member2;
    public Member member3;
    public Member member4;
    public Member member5;
    public Member member6;

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
        broker1 = new Broker(Util.getSimpleServerSocket(brokerAddr1));
        broker2 = new Broker(Util.getSimpleServerSocket(brokerAddr2));
        broker3 = new Broker(Util.getSimpleServerSocket(brokerAddr3));
        for (var broker: I(broker1, broker2, broker3)) {
            broker.startAccept().await();
        }
        broker1.linkBroker(brokerAddr2);
        broker2.linkBroker(brokerAddr3);
        member1 = new Member(brokerAddr1);
        member2 = new Member(brokerAddr1);
        member3 = new Member(brokerAddr2);
        member4 = new Member(brokerAddr3);
        member5 = new Member(brokerAddr3);
        member6 = new Member(brokerAddr1);
    }
    @org.junit.After
    public void tearDown() {
        for (var member: I(member1, member2, member3, member4, member5, member6)) {
            member.close();
        }
        for (var broker: I(broker1, broker2, broker3)) {
            broker.stopAccept().await();
            uncheck(() -> broker.getNetServer().getSocket().close());
        }
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
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubSub(member1, member1);
    }
    @org.junit.Test
    public void testPubSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubSub(member1, member2);
    }
    @org.junit.Test
    public void testPubSubMultiBrokerOneJump() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubSub(member1, member3);
    }
    @org.junit.Test
    public void testPubSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_TWO_JUMPS);
        testPubSub(member1, member4);
        testPubSub(member5, member6);
    }
    @org.junit.Test
    public void testPubNoSubSameMember() {
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubNoSub(member1, member1);
    }
    @org.junit.Test
    public void testPubNoSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubNoSub(member1, member2);
    }
    @org.junit.Test
    public void testPubNoSubMultiBrokerOneJump() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSub(member1, member3);
    }
    @org.junit.Test
    public void testPubNoSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_TWO_JUMPS);
        testPubNoSub(member1, member4);
        testPubNoSub(member5, member6);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameMember() {
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubNoSubThenSub(member1, member1);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubNoSubThenSub(member1, member2);
    }
    @org.junit.Test
    public void testPubNoSubThenSubMultiBrokerOneJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSubThenSub(member1, member3);
    }
    @org.junit.Test
    public void testPubNoSubThenSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSubThenSub(member1, member3);
        testPubNoSubThenSub(member5, member6);
    }
}
