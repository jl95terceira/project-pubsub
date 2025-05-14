package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import jl95.lang.I;
import jl95.net.io.util.Util;
import jl95.net.pubsub.collections.MemberAdaptersCollection;
import jl95.Parameters;

public class Test1To1 {

    public static boolean TEST_SAME_MEMBER  = true;
    public static boolean TEST_SAME_BROKER  = true;
    public static boolean TEST_MULTI_BROKER_ONE_JUMP  = true;
    public static boolean TEST_MULTI_BROKER_TWO_JUMPS = true;

    public Instant t0;
    public final InetSocketAddress brokerAddr1 = new InetSocketAddress("127.0.0.1", 42421);
    public final InetSocketAddress brokerAddr2 = new InetSocketAddress("127.0.0.1", 42422);
    public final InetSocketAddress brokerAddr3 = new InetSocketAddress("127.0.0.1", 42423);
    public List<Broker> brokersList;
    public Broker broker1;
    public Broker broker2;
    public Broker broker3;
    public List<Member> membersList;
    public Member member1OfBroker1;
    public Member member2OfBroker1;
    public Member member1OfBroker2;
    public Member member2OfBroker2;
    public Member member1OfBroker3;
    public Member member2OfBroker3;
    public Member member3OfBroker1;

    private void   zzz() {
        sleep(Parameters.zzzDuration);
    }
    private Broker managed(Broker broker) {
        brokersList.add(broker);
        return broker;
    }
    private Member managed(Member member) {
        membersList.add(member);
        return member;
    }
    private void   initBrokers   () {
        brokersList = new ArrayList<>(3);
        broker1 = managed(new Broker(Util.getSimpleServerSocket(brokerAddr1)));
        broker2 = managed(new Broker(Util.getSimpleServerSocket(brokerAddr2)));
        broker3 = managed(new Broker(Util.getSimpleServerSocket(brokerAddr3)));
        for (var broker: brokersList) {
            System.out.printf("Broker (%s) start ...", broker.getBrokerId());
            broker.startAccept().await();
            System.out.println(" start OK");
        }
        broker1.linkBroker(brokerAddr2);
        System.out.printf("Broker (%s) linked to broker (%s)\n", broker1.getBrokerId(), broker2.getBrokerId());
        broker2.linkBroker(brokerAddr3);
        System.out.printf("Broker (%s) linked to broker (%s)\n", broker2.getBrokerId(), broker3.getBrokerId());
    }
    private void   restartBrokers() {
        for (var broker: brokersList) {
            broker.stopAccept();
            broker.close();
        }
        initBrokers();
    }
    private void   initMembers   () {
        membersList = new ArrayList<>(7);
        member1OfBroker1 = managed(Member.of(brokerAddr1));
        member2OfBroker1 = managed(Member.of(brokerAddr1));
        member1OfBroker2 = managed(Member.of(brokerAddr2));
        member2OfBroker2 = managed(Member.of(brokerAddr2));
        member1OfBroker3 = managed(Member.of(brokerAddr3));
        member2OfBroker3 = managed(Member.of(brokerAddr3));
        member3OfBroker1 = managed(Member.of(brokerAddr1));
        for (var member: membersList) {
            System.out.printf("Member (%s) connecting ...", member.getMemberId());
            member.connect();
            System.out.println(" connection OK");
        }
    }

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
        initBrokers();
        sleep(1000);
        initMembers();
    }
    @org.junit.After
    public void tearDown() {
        for (var member: I.of(membersList).filter(Objects::nonNull)) {
            member.close();
        }
        for (var broker: I.of(brokersList)) {
            broker.stopAccept().await();
            uncheck(() -> broker.getNetServer().getSocket().close());
        }
    }

    public final void testPubSub         (Member producerMember, Member consumerMember) {
        var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            zzz();
            System.out.println("subscribe");
            consumerMember.subscribeByList(I("foo"));
            zzz();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
            zzz();
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
            consumerMember.consumeStop().await();
        });
        m.accept();
        m.accept();
    }
    public final void testPubNoSub       (Member producerMember, Member consumerMember) {
        var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            zzz();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
            zzz();
            System.out.println("assert consumed");
            org.junit.Assert.assertFalse(msgFuture.isDone());
            consumerMember.consumeStop().await();
        });
        m.accept();
        m.accept();
    }
    public final void testPubNoSubThenSub(Member producerMember, Member consumerMember) {
        var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            zzz();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
            zzz();
            System.out.println("assert NOT consumed (since not subscribed)");
            org.junit.Assert.assertFalse(msgFuture.isDone());
            System.out.println("subscribe");
            consumerMember.subscribeByList(I("foo"));
            zzz();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember).produce("foo", "BAR");
            zzz();
            System.out.println("assert consumed");
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
            consumerMember.consumeStop().await();
            consumerMember.subscribeToNone();
            System.out.println("done");
        });
        m.accept();
        m.accept();
    }

    @org.junit.Test
    public void testConnectAndClose() {
    }
    @org.junit.Test
    public void testPubSubSameMember() {
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubSub(member1OfBroker1, member1OfBroker1);
    }
    @org.junit.Test
    public void testPubSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubSub(member1OfBroker1, member2OfBroker1);
    }
    @org.junit.Test
    public void testPubSubMultiBrokerOneJump() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubSub(member1OfBroker1, member1OfBroker2);
        testPubSub(member2OfBroker2, member1OfBroker3);
    }
    @org.junit.Test
    public void testPubSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_TWO_JUMPS);
        testPubSub(member1OfBroker1, member1OfBroker3);
        testPubSub(member2OfBroker3, member3OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubSameMember() {
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubNoSub(member1OfBroker1, member1OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubNoSub(member1OfBroker1, member2OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubMultiBrokerOneJump() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSub(member1OfBroker1, member1OfBroker2);
        testPubNoSub(member2OfBroker2, member1OfBroker3);
    }
    @org.junit.Test
    public void testPubNoSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_TWO_JUMPS);
        testPubNoSub(member1OfBroker1, member1OfBroker3);
        testPubNoSub(member2OfBroker3, member3OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameMember() {
        org.junit.Assume.assumeTrue(TEST_SAME_MEMBER);
        testPubNoSubThenSub(member1OfBroker1, member1OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubThenSubSameBroker() {
        org.junit.Assume.assumeTrue(TEST_SAME_BROKER);
        testPubNoSubThenSub(member1OfBroker1, member2OfBroker1);
    }
    @org.junit.Test
    public void testPubNoSubThenSubMultiBrokerOneJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSubThenSub(member1OfBroker1, member1OfBroker2);
        testPubNoSubThenSub(member2OfBroker2, member1OfBroker3);
    }
    @org.junit.Test
    public void testPubNoSubThenSubMultiBrokerTwoJumps() {
        org.junit.Assume.assumeTrue(TEST_MULTI_BROKER_ONE_JUMP);
        testPubNoSubThenSub(member1OfBroker1, member1OfBroker2);
        testPubNoSubThenSub(member2OfBroker3, member3OfBroker1);
    }
}
