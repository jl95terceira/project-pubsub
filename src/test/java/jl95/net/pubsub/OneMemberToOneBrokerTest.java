package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jl95.Parameters;
import jl95.lang.I;
import jl95.net.pubsub.collections.MemberAdaptersCollection;

public class OneMemberToOneBrokerTest extends Test {

    public static class NumberedBroker extends Broker {
        public final int nr;
        public NumberedBroker(int nr, ServerSocket serverSocket) {
            super(serverSocket);
            this.nr = nr;
        }
    }
    public static class NumberedMember extends Member {
        public final int nr;
        public NumberedMember(int nr, InetSocketAddress addr) {
            super(I(addr));
            this.nr = nr;
        }
    }

    public static boolean TEST_SAME_MEMBER  = true;
    public static boolean TEST_SAME_BROKER  = true;
    public static boolean TEST_MULTI_BROKER_ONE_JUMP  = true;
    public static boolean TEST_MULTI_BROKER_TWO_JUMPS = true;

    public Instant t0;
    public final InetSocketAddress brokerAddr1 = new InetSocketAddress("127.0.0.1", 42421);
    public final InetSocketAddress brokerAddr2 = new InetSocketAddress("127.0.0.1", 42422);
    public final InetSocketAddress brokerAddr3 = new InetSocketAddress("127.0.0.1", 42423);
    public List<NumberedBroker> brokersList;
    public NumberedBroker broker1;
    public NumberedBroker broker2;
    public NumberedBroker broker3;
    public List<NumberedMember> membersList;
    public NumberedMember member1OfBroker1;
    public NumberedMember member2OfBroker1;
    public NumberedMember member1OfBroker2;
    public NumberedMember member2OfBroker2;
    public NumberedMember member1OfBroker3;
    public NumberedMember member2OfBroker3;
    public NumberedMember member3OfBroker1;

    private NumberedBroker managed(NumberedBroker broker) {
        brokersList.add(broker);
        return broker;
    }
    private NumberedMember managed(NumberedMember member) {
        membersList.add(member);
        return member;
    }
    private void   initBrokers   () {
        brokersList = new ArrayList<>(3);
        broker1 = managed(new NumberedBroker(1, jl95.net.Util.getSimpleServerSocket(brokerAddr1)));
        broker2 = managed(new NumberedBroker(2, jl95.net.Util.getSimpleServerSocket(brokerAddr2)));
        broker3 = managed(new NumberedBroker(3, jl95.net.Util.getSimpleServerSocket(brokerAddr3)));
        for (final var broker: brokersList) {
            System.out.printf("Broker %s start ...", broker.nr);
            broker.startAccept().get();
            System.out.println(" start OK");
        }
        broker1.linkBroker(brokerAddr2);
        System.out.printf("Broker %s linked to broker %s\n", broker1.nr, broker2.nr);
        broker2.linkBroker(brokerAddr3);
        System.out.printf("Broker %s linked to broker %s\n", broker2.nr, broker3.nr);
    }
    private void   restartBrokers() {
        for (final var broker: brokersList) {
            broker.stopAccept();
            broker.close();
            System.out.printf("Broker %s closed\n", broker.nr);
        }
        initBrokers();
    }
    private void   initMembers   () {
        final int membersCount = 7;
        membersList = new ArrayList<>(membersCount);
        final Iterator<Integer> it = I.range(7).map(i -> 1+i).iterator();
        member1OfBroker1 = managed(new NumberedMember(it.next(), brokerAddr1));
        member2OfBroker1 = managed(new NumberedMember(it.next(), brokerAddr1));
        member1OfBroker2 = managed(new NumberedMember(it.next(), brokerAddr2));
        member2OfBroker2 = managed(new NumberedMember(it.next(), brokerAddr2));
        member1OfBroker3 = managed(new NumberedMember(it.next(), brokerAddr3));
        member2OfBroker3 = managed(new NumberedMember(it.next(), brokerAddr3));
        member3OfBroker1 = managed(new NumberedMember(it.next(), brokerAddr1));
        for (var member: membersList) {
            System.out.printf("Member %s connecting ...", member.nr);
            member.connect();
            System.out.println(" connection OK");
        }
    }

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
        initBrokers();
        sleepLong();
        initMembers();
    }
    @org.junit.After
    public void tearDown() {
        for (var member: I.of(membersList).filter(Objects::nonNull)) {
            member.close();
            System.out.printf("Member %s closed\n", member.nr);
        }
        for (var broker: I.of(brokersList)) {
            broker.stopAccept().get();
            uncheck(() -> broker.getNetServer().getSocket().close());
        }
    }

    public final void testPubSub         (Member producerMember, Member consumerMember) {
        var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember)
                    .consume((topic, payload) -> {
                        msgFuture.complete(payload);
                    });
            sleepShort();
            System.out.println("subscribe");
            consumerMember.subscribeByList(I("foo"))
                    .get(Parameters.waitSubscribeSeconds, TimeUnit.SECONDS);
            sleepShort();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember)
                    .produce("foo", "BAR")
                    .get(Parameters.waitProduceSeconds, TimeUnit.SECONDS);
            sleepShort();
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
            consumerMember.consumeStop().get();
        });
        m.accept();
        m.accept();
    }
    public final void testPubNoSub       (Member producerMember, Member consumerMember) {
        final var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            sleepShort();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember)
                    .produce("foo", "BAR")
                    .get(Parameters.waitProduceSeconds, TimeUnit.SECONDS);
            sleepLong();
            System.out.println("assert consumed");
            org.junit.Assert.assertFalse(msgFuture.isDone());
            consumerMember.consumeStop().get();
        });
        I.range(2).forEach(i -> m.accept());
    }
    public final void testPubNoSubThenSub(Member producerMember, Member consumerMember) {
        var m = method(() -> {
            var msgFuture = new CompletableFuture<String>();
            System.out.println("consume");
            MemberAdaptersCollection.getStringConsumer(consumerMember).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            sleepShort();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember)
                    .produce("foo", "BAR")
                    .get(Parameters.waitProduceSeconds, TimeUnit.SECONDS);
            sleepLong();
            System.out.println("assert NOT consumed (since not subscribed)");
            org.junit.Assert.assertFalse(msgFuture.isDone());
            System.out.println("subscribe");
            consumerMember.subscribeByList(I("foo"))
                    .get(Parameters.waitSubscribeSeconds, TimeUnit.SECONDS);
            sleepShort();
            System.out.println("produce");
            MemberAdaptersCollection.getStringProducer(producerMember)
                    .produce("foo", "BAR")
                    .get(Parameters.waitProduceSeconds, TimeUnit.SECONDS);
            sleepShort();
            System.out.println("assert consumed");
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
            consumerMember.consumeStop().get();
            consumerMember.subscribeToNone()
                    .get(Parameters.waitSubscribeSeconds, TimeUnit.SECONDS);
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
