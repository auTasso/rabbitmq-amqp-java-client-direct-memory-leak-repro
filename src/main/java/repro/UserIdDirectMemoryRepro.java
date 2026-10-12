package repro;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import io.netty.buffer.PooledByteBufAllocator;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Publishes 512 B messages at 5,000 msg/s and consumes them in the same JVM, then prints direct memory
 * and GC count every second. With user-id set, direct memory grows until the consumer fails.
 */
public class UserIdDirectMemoryRepro {
    static final String URI = "amqp://repro:repro@localhost:5672/%2F";
    static final String QUEUE = "user-id-repro";
    static final int RATE = 5_000;
    static final int SECONDS = 30;

    public static void main(String[] args) {
        boolean withUserId = args.length == 0 || Boolean.parseBoolean(args[0]);
        Environment env = new AmqpEnvironmentBuilder().connectionSettings().uri(URI).environmentBuilder().build();
        Connection consumerConnection = env.connectionBuilder().build();
        Connection publisherConnection = env.connectionBuilder().build();
        Management management = consumerConnection.management();
        management.queue(QUEUE).type(Management.QueueType.CLASSIC).declare();
        management.queuePurge(QUEUE);

        AtomicLong received = new AtomicLong();
        consumerConnection.consumerBuilder().queue(QUEUE)
            .messageHandler((ctx, message) -> {
                message.body();
                ctx.accept();
                received.incrementAndGet();
            })
            .build();

        Thread publisher = new Thread(() -> publish(publisherConnection, withUserId));
        publisher.setDaemon(true);
        publisher.start();

        System.out.printf("user-id set: %b%n", withUserId);
        System.out.println("s  received  direct_MiB  heap_MiB  gc_count");
        for (int s = 1; s <= SECONDS; s++) {
            LockSupport.parkNanos(1_000_000_000L);
            System.out.printf("%-2d %8d  %10d  %8d  %8d%n", s, received.get(),
                PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory() >> 20,
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() >> 20,
                ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum());
        }
        publisher.interrupt();
        env.close();
    }

    static void publish(Connection connection, boolean withUserId) {
        Publisher publisher = connection.publisherBuilder().queue(QUEUE).build();
        byte[] body = new byte[512];
        byte[] userId = "repro".getBytes(StandardCharsets.UTF_8);
        long start = System.nanoTime();
        for (long ms = 0; !Thread.currentThread().isInterrupted(); ms++) {
            LockSupport.parkNanos(start + ms * 1_000_000L - System.nanoTime());
            for (int i = 0; i < RATE / 1_000; i++) {
                var message = publisher.message(body);
                if (withUserId) message.userId(userId);
                publisher.publish(message, ctx -> { });
            }
        }
    }
}
