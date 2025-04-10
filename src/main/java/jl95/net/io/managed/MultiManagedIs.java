package jl95.net.io.managed;

import static jl95.lang.SuperPowers.strict;

import java.io.InputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import jl95.lang.AutoMapper;
import jl95.lang.AutoMappersCollection;
import jl95.lang.StrictMap;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;
import jl95.net.io.Is;

public class MultiManagedIs implements ManagedIs {

    private final StrictMap <UUID, ManagedIs> misMap        = strict(new ConcurrentHashMap<>());
    private final AutoMapper<UUID, ManagedIs> misAutoMapper = AutoMappersCollection.getUuidAutoMapper(misMap);

    public final UUID      add   (ManagedIs mis) {
        return misAutoMapper.put(mis);
    }
    public final ManagedIs remove(UUID      id) {
        return misMap.remove(id);
    }

    @Override
    public <T> T withInput(Function1<T, InputStream> f) {
        for (var mis: misMap.values()) {
            mis.withInput(f);
        }
    }
}
