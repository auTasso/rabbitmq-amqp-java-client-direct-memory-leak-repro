# `user-id` pins pooled direct memory in amqp-client

A consumer that receives messages with `properties.user-id` set keeps each Netty read buffer
alive until the next GC. At 5,000 msg/s, direct memory grows by ~90 MiB/s and the connection
fails with `failed to allocate 4194304 byte(s) of direct memory` before any GC runs. Without
`user-id`, direct memory stays flat.

One class, one JVM: a publisher and a consumer on separate connections, 512 B bodies at
5,000 msg/s. It prints direct memory, heap and GC count every second for 30 s.

## Run

```sh
docker run -d --name user-id-repro -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=repro -e RABBITMQ_DEFAULT_PASS=repro \
  rabbitmq:4.1.4-management

mvn compile exec:exec                  # with user-id
mvn compile exec:exec -DuserId=false   # control, without user-id
```

The broker checks `user-id` against the connection user, so both are `repro`. The JVM runs with
`-Xmx1g -Xmn768m -XX:MaxDirectMemorySize=1g` (see `pom.xml`). The large young generation keeps GC
away for the duration of the run, which is what makes the failure visible.

## Expected output

With `user-id`:

```text
user-id set: true
s  received  direct_MiB  heap_MiB  gc_count
1      4980         108        84         0
2     10015         200       112         0
...
10    50050         888       339         0
11    55070         972       368         0
[...] WARN com.rabbitmq.qpid.protonj2.client.impl.ClientConnection - Connection ID:... has failed due to: ClientConnectionRemotelyClosedException -> failed to allocate 4194304 byte(s) of direct memory (used: 1073741824, max: 1073741824)
12    58560        1024       395         0
13    58560        1024       407         0
...
30    58560        1024       625         0
```

Without `user-id`:

```text
user-id set: false
s  received  direct_MiB  heap_MiB  gc_count
1      4980           8        81         0
...
28   140120           8       767         0
29   145125           8        36         1
30   150130           8        60         1
```

## Cause

Links at tag `v1.6.0`, protonj2 as bundled under `src/main/qpid`.

1. [`TcpTransport.java:432`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/client/transport/netty4/TcpTransport.java#L432)
   sets `PooledByteBufAllocator.DEFAULT`, and
   [`TcpTransport.java:440`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/client/transport/netty4/TcpTransport.java#L438-L441)
   a `FixedRecvByteBufAllocator` of 64 KiB
   ([`TransportOptions.java:31`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/client/TransportOptions.java#L31)):
   every socket read lands in a 64 KiB pooled direct buffer.
2. [`ProtonFrameDecodingHandler.java:406`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/engine/impl/ProtonFrameDecodingHandler.java#L395-L407)
   takes the frame payload as `copy(..., readOnly=true)`, a read-only view of the read buffer.
3. [`AbstractBinaryTypeDecoder.java:64`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/codec/decoders/primitives/AbstractBinaryTypeDecoder.java#L64)
   decodes every AMQP `binary`, `user-id` included, with `copy(..., readOnly=true)` again.
4. [`Netty4ToProtonBufferAdapter.java:437`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/buffer/netty/Netty4ToProtonBufferAdapter.java#L437-L438)
   turns a read-only copy of a read-only source into `retainedSlice`, so no copy is made.
5. [`Binary.java:55`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/types/Binary.java#L48-L57)
   releases that slice only through a `Cleaner`, i.e. after the `Binary` is garbage collected.

An 8 B `user-id` therefore holds a reference on a 64 KiB read buffer, shared by the few messages of
that read. Netty's pooled
allocator never triggers a GC when it reaches the direct memory limit, unlike
`ByteBuffer.allocateDirect`, so it fails while the heap is far from full.

The body is not affected:
[`DataTypeDecoder.java:84`](https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/v1.6.0/src/main/qpid/org/apache/qpid/protonj2/codec/decoders/messaging/DataTypeDecoder.java#L84-L87)
copies it into a heap buffer "to avoid retaining any pooled buffers for prolonged periods of time".

Over TLS the same happens at a lower rate: `SslHandler` decrypts into a buffer sized to the
decrypted bytes, so each `user-id` pins less (~0.8 KiB vs ~27 KiB per message, measured with a
512 B body).

## Suggested fix

Copy `binary` values into a heap buffer in `AbstractBinaryTypeDecoder`, as `DataTypeDecoder`
already does for the body.

## Tested with

| Component | Version |
| --- | --- |
| amqp-client | 1.6.0 (1.5.0 behaves the same) |
| Netty | 4.2.19.Final, as declared by amqp-client |
| JDK | 25.0.2 |
| Maven | 3.6.3 |
| RabbitMQ | 4.1.4 |
