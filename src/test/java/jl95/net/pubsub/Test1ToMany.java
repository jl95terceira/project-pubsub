package jl95.net.pubsub;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.method;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.tuple;
import static jl95.lang.SuperPowers.uncheck;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import jl95.net.pubsub.collections.MemberAdaptersCollection;
import jl95.Parameters;

public class Test1ToMany {

    public static InetSocketAddress BROKER_1_ADDRESS = new InetSocketAddress("127.0.0.1", 421);
    public static InetSocketAddress BROKER_2_ADDRESS = new InetSocketAddress("127.0.0.1", 422);
    public static String            TOPIC_NAME       = "test";

    public Broker broker1;
    public Broker broker2;
    public Member memberOfBroker1;
    public Member memberOfBroker2;
    public Member memberOfBothBrokers;

    private void zzz() {
        sleep(Parameters.zzzDuration);
    }
    private void initBroker1() {

        broker1 = new Broker(BROKER_1_ADDRESS);
        broker1.startAccept();
    }
    private void initBroker2() {

        broker2 = new Broker(BROKER_2_ADDRESS);
        broker2.startAccept();
    }

    @org.junit.Before
    public void setup() {
        initBroker1();
        System.out.println("Broker 1 UP");
        initBroker2();
        System.out.println("Broker 2 UP");
        sleep(1000);
        memberOfBroker1     = Member.of(BROKER_1_ADDRESS);
        System.out.print("Member of broker 1 UP");
        memberOfBroker1.connect();
        System.out.println(" and CONNECTED");
        memberOfBroker2     = Member.of(BROKER_2_ADDRESS);
        System.out.print("Member of broker 2 UP");
        memberOfBroker2.connect();
        System.out.println(" and CONNECTED");
        memberOfBothBrokers = Member.of(BROKER_1_ADDRESS, BROKER_2_ADDRESS);
        System.out.print("Member of both brokers UP");
        memberOfBothBrokers.connect();
        System.out.print(" and CONNECTED");
        memberOfBothBrokers.subscribeByList(I(TOPIC_NAME));
        System.out.println(" and SUBSCRIBED");
        memberOfBothBrokers.consume();
        System.out.println(" and CONSUMING");
        zzz();
    }
    @org.junit.After
    public void teardown() {
        System.out.println("Closing");
        for (var t: I(
            tuple("Broker 1",               method(broker1            ::close)),
            tuple("Broker 2",               method(broker2            ::close)),
            tuple("Member of broker 1",     method(memberOfBroker1    ::close)),
            tuple("Member of broker 2",     method(memberOfBroker2    ::close)),
            tuple("Member of both brokers", method(memberOfBothBrokers::close))
        )){
            t.a2.accept();
            System.out.printf("%s CLOSED\n", t.a1);
        }
    }

    public void go() {
        for (var t: I(
            tuple(memberOfBroker1, "Member of broker 1"),
            tuple(memberOfBroker2, "Member of broker 2")
        )) {
            var member = t.a1;
            var descr  = t.a2;
            System.out.printf("Testing with %s\n", descr);
            var msg    = "hello, %s".formatted(UUID.randomUUID().toString());
            var msgPromise = new CompletableFuture<String>();
            MemberAdaptersCollection.getStringConsumer(memberOfBothBrokers)
                .onConsumed((topic, msg_) -> msgPromise.complete(msg_));
            MemberAdaptersCollection.getStringProducer(member)
                .produce(TOPIC_NAME, msg);
            System.out.println("Produced - waiting for consumed back");
            org.junit.Assert.assertEquals(msg, uncheck(() -> msgPromise.get()));
            System.out.println("OK");
        }
    }

    @org.junit.Test
    public void testOpenClose() {}
    @org.junit.Test
    public void testNoInterrupt() {

        go();
        go();
    }
    @org.junit.Test
    public void testInterruptSoft() {

        go();
        for (var broker: I(broker1, broker2)) {
            broker.resetConnections();
        }
        go();
    }
    @org.junit.Test
    public void testInterruptHard() {

        go();
        for (var broker: I(broker1, broker2)) {
            broker.close();
        }
        sleep(Parameters.zzzLongDuration);
        initBroker1();
        initBroker2();
        go();
    }
}
