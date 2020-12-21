package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._1_ignite_token_bucket;

import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._1_ignite_token_bucket.remote.BucketState;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.gridkit.nanocloud.Cloud;
import org.gridkit.nanocloud.CloudFactory;
import org.gridkit.nanocloud.VX;
import org.gridkit.vicluster.ViNode;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class IgniteTokenBucketTest {

    private static final int WARMUP_SECONDS = 3;

    private static Cloud cloud;
    private static ViNode server;
    private static Ignite ignite;

    public static void main(String[] args) throws InterruptedException {
        IgniteCache<String, BucketState> cache = createCache();

        // 100 tokens per 1 second
        IgniteTokenBucket limiter = new IgniteTokenBucket(100L, Duration.ofSeconds(1), "42", cache);

        AtomicLong consumed = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        initLogging(consumed, rejected);

        while (true) {
            if (limiter.tryAcquire(1)) {
                consumed.addAndGet(1);
            } else {
                rejected.addAndGet(1);
            }
        }
    }

    private static void initLogging(AtomicLong consumed, AtomicLong rejected) {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            System.out.printf("Consumed %d, Rejected %d\n", consumed.getAndSet(0), rejected.getAndSet(0));
        }, WARMUP_SECONDS, 1L, TimeUnit.SECONDS);
    }

    private static IgniteCache<String, BucketState> createCache() {
        // start separated JVM on current host
        cloud = CloudFactory.createCloud();
        cloud.node("**").x(VX.TYPE).setLocal();
        server = cloud.node("stateful-ignite-server");

        int serverDiscoveryPort = 47500;
        String serverNodeAdress = "localhost:" + serverDiscoveryPort;

        server.exec((Runnable & Serializable) () -> {
            TcpDiscoveryVmIpFinder neverFindOthers = new TcpDiscoveryVmIpFinder();
            neverFindOthers.setAddresses(Collections.singleton(serverNodeAdress));

            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
            tcpDiscoverySpi.setIpFinder(neverFindOthers);
            tcpDiscoverySpi.setLocalPort(serverDiscoveryPort);

            IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
            igniteConfiguration.setClientMode(false);
            igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);

            CacheConfiguration cacheConfiguration = new CacheConfiguration("my_buckets");
            Ignite ignite = Ignition.start(igniteConfiguration);
            ignite.getOrCreateCache(cacheConfiguration);
        });

        // start ignite client which works inside current JVM and does not hold data
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton(serverNodeAdress));
        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);
        igniteConfiguration.setClientMode(true);
        ignite = Ignition.start(igniteConfiguration);
        CacheConfiguration cacheConfiguration = new CacheConfiguration("my_buckets");
        return ignite.getOrCreateCache(cacheConfiguration);
    }

}
