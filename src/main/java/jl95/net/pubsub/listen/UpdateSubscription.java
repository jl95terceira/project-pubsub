package jl95.net.pubsub.listen;

import java.util.UUID;

import jl95.net.pubsub.Subscription;

public record UpdateSubscription(UUID memberId, Subscription sub) {}

