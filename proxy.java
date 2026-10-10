import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class proxy {
    private static final int DEFAULT_RECV_BUFFER = 1024 * 1024;
    private static final int MIN_RECV_BUFFER = 128 * 1024;
    private static final long DATA_SEED_XOR = 0x44415441L;
    private static final long ACK_SEED_XOR = 0x0041434bL;

    private final int listenPort;
    private final String receiverHost;
    private final int receiverPort;
    private final long seed;
    private final int dataLossPercent;
    private final int ackLossPercent;
    private final long delayMs;
    private final long jitterMs;
    private final int reorderPercent;
    private final long reorderExtraMs;
    private final long bandwidthKbps;
    private final Set<Integer> dropDataOnce = ConcurrentHashMap.newKeySet();
    private final Set<Integer> dropAckOnce = ConcurrentHashMap.newKeySet();
    private final boolean verbose;

    private DatagramSocket clientSocket;
    private DatagramSocket receiverSocket;
    private final AtomicReference<Endpoint> activeSenderEndpoint = new AtomicReference<>();
    private final DelayQueue<DeliveryItem> dataQueue = new DelayQueue<>();
    private final DelayQueue<DeliveryItem> ackQueue = new DelayQueue<>();
    private final AtomicLong dataOrdinal = new AtomicLong(1);
    private final AtomicLong ackOrdinal = new AtomicLong(1);
    private final Random dataRandom;
    private final Random ackRandom;
    private final AtomicReference<Endpoint> receiverEndpoint = new AtomicReference<>();
    private volatile boolean running = true;
    private volatile long directionNextAvailableData = 0;
    private volatile long directionNextAvailableAck = 0;

    public static void main(String[] args) {
        try {
            proxy p = new proxy(parseArgs(args));
            p.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    public proxy(Args args) {
        this.listenPort = args.listenPort;
        this.receiverHost = args.receiverHost;
        this.receiverPort = args.receiverPort;
        this.seed = args.seed;
        this.dataLossPercent = args.dataLoss;
        this.ackLossPercent = args.ackLoss;
        this.delayMs = args.delayMs;
        this.jitterMs = args.jitterMs;
        this.reorderPercent = args.reorderPercent;
        this.reorderExtraMs = args.reorderExtraMs;
        this.bandwidthKbps = args.bandwidthKbps;
        this.dropDataOnce.addAll(args.dropDataOnce);
        this.dropAckOnce.addAll(args.dropAckOnce);
        this.verbose = args.verbose;
        this.dataRandom = new Random(seed ^ DATA_SEED_XOR);
        this.ackRandom = new Random(seed ^ ACK_SEED_XOR);
    }

    private static Args parseArgs(String[] args) throws IllegalArgumentException {
        if (args.length < 4) {
            throw new IllegalArgumentException("Usage: java proxy.java --listen-port <port> --receiver-host <host> --receiver-port <port> [options]");
        }

        int listenPort = -1;
        String receiverHost = null;
        int receiverPort = -1;
        long seed = System.nanoTime();
        int dataLoss = 0;
        int ackLoss = 0;
        long delayMs = 0;
        long jitterMs = 0;
        int reorderPercent = 0;
        long reorderExtraMs = 0;
        long bandwidthKbps = 0;
        List<Integer> dropDataOnce = new ArrayList<>();
        List<Integer> dropAckOnce = new ArrayList<>();
        boolean verbose = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--listen-port":
                    listenPort = parseIntPort(args[++i]);
                    break;
                case "--receiver-host":
                    receiverHost = args[++i];
                    break;
                case "--receiver-port":
                    receiverPort = parseIntPort(args[++i]);
                    break;
                case "--seed":
                    seed = Long.parseLong(args[++i]);
                    break;
                case "--data-loss":
                    dataLoss = parseInt0to100(args[++i]);
                    break;
                case "--ack-loss":
                    ackLoss = parseInt0to100(args[++i]);
                    break;
                case "--delay-ms":
                    delayMs = parseNonNegativeLong(args[++i]);
                    break;
                case "--jitter-ms":
                    jitterMs = parseNonNegativeLong(args[++i]);
                    break;
                case "--reorder-percent":
                    reorderPercent = parseInt0to100(args[++i]);
                    break;
                case "--reorder-extra-ms":
                    reorderExtraMs = parseNonNegativeLong(args[++i]);
                    break;
                case "--bandwidth-kbps":
                    bandwidthKbps = parseNonNegativeLong(args[++i]);
                    if (bandwidthKbps > 1000000) {
                        throw new IllegalArgumentException("Bandwidth too high: " + args[i]);
                    }
                    break;
                case "--drop-data-once":
                    dropDataOnce.addAll(parseCommaSeparatedInts(args[++i]));
                    break;
                case "--drop-ack-once":
                    dropAckOnce.addAll(parseCommaSeparatedInts(args[++i]));
                    break;
                case "--verbose":
                    verbose = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }

        if (listenPort < 0 || receiverHost == null || receiverPort < 0) {
            throw new IllegalArgumentException("Missing required options");
        }

        return new Args(listenPort, receiverHost, receiverPort, seed, dataLoss, ackLoss, delayMs, jitterMs, reorderPercent, reorderExtraMs, bandwidthKbps, dropDataOnce, dropAckOnce, verbose);
    }

    private static int parseIntPort(String s) {
        int v = Integer.parseInt(s);
        if (v < 0 || v > 65535) throw new IllegalArgumentException("Invalid port: " + s);
        return v;
    }

    private static int parseInt0to100(String s) {
        int v = Integer.parseInt(s);
        if (v < 0 || v > 100) throw new IllegalArgumentException("Invalid percentage: " + s);
        return v;
    }

    private static long parseNonNegativeLong(String s) {
        long v = Long.parseLong(s);
        if (v < 0) throw new IllegalArgumentException("Invalid value: " + s);
        return v;
    }

    private static List<Integer> parseCommaSeparatedInts(String s) {
        List<Integer> result = new ArrayList<>();
        for (String part : s.split(",")) {
            result.add(Integer.parseInt(part.trim()));
        }
        return result;
    }

    public void run() {
        try {
            clientSocket = new DatagramSocket(listenPort);
            receiverSocket = new DatagramSocket();

            clientSocket.setReceiveBufferSize(DEFAULT_RECV_BUFFER);
            receiverSocket.setReceiveBufferSize(DEFAULT_RECV_BUFFER);
            int bufferSize = clientSocket.getReceiveBufferSize();
            if (bufferSize < MIN_RECV_BUFFER) {
                throw new IOException("Cannot allocate receive buffer of at least " + MIN_RECV_BUFFER + " bytes");
            }

            receiverEndpoint.set(new Endpoint(receiverHost, receiverPort));
            long actualClientPort = clientSocket.getLocalPort();

            System.out.println("PROXY_READY port=" + actualClientPort + " receiver_port=" + receiverPort);
            System.out.flush();

            Thread senderReceiver = new Thread(() -> receiveFromSender());
            Thread receiverReceiver = new Thread(() -> receiveFromReceiver());
            Thread dataDelivery = new Thread(() -> runDelivery("data", dataQueue));
            Thread ackDelivery = new Thread(() -> runDelivery("ack", ackQueue));

            senderReceiver.start();
            receiverReceiver.start();
            dataDelivery.start();
            ackDelivery.start();

            senderReceiver.join();
            receiverReceiver.join();
            dataDelivery.join();
            ackDelivery.join();

        } catch (SocketException e) {
            if (running) {
                System.err.println("Socket error: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        running = false;
        if (clientSocket != null && !clientSocket.isClosed()) {
            clientSocket.close();
        }
        if (receiverSocket != null && !receiverSocket.isClosed()) {
            receiverSocket.close();
        }
    }

    private void receiveFromSender() {
        byte[] buffer = new byte[65535];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            while (running) {
                packet.setLength(buffer.length);
                clientSocket.receive(packet);
                Endpoint source = new Endpoint(packet.getAddress(), packet.getPort());
                byte[] wireBytes = Arrays.copyOf(packet.getData(), packet.getLength());

                String msg = new String(wireBytes, java.nio.charset.StandardCharsets.UTF_8);
                String keyword = msg.split(" ", 2)[0];

                if (keyword.equals("BEGIN")) {
                    activeSenderEndpoint.set(source);
                    forwardControl(wireBytes);
                } else if (keyword.equals("END")) {
                    forwardControl(wireBytes);
                } else if (keyword.equals("DATA")) {
                    decideAndEnqueue("data", wireBytes, parseDataSeqnum(msg));
                }
            }
        } catch (SocketException e) {
            if (running) {
                if (verbose) System.err.println("Sender receive error: " + e.getMessage());
            }
        } catch (IOException e) {
            if (verbose) System.err.println("Sender IO error: " + e.getMessage());
        }
    }

    private void receiveFromReceiver() {
        byte[] buffer = new byte[65535];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            while (running) {
                packet.setLength(buffer.length);
                receiverSocket.receive(packet);
                Endpoint source = new Endpoint(packet.getAddress(), packet.getPort());
                byte[] wireBytes = Arrays.copyOf(packet.getData(), packet.getLength());

                String msg = new String(wireBytes, java.nio.charset.StandardCharsets.UTF_8);
                String keyword = msg.split(" ", 2)[0];

                if (keyword.equals("ACK")) {
                    Endpoint expectedReceiver = receiverEndpoint.get();
                    if (expectedReceiver == null || !source.equals(expectedReceiver)) {
                        if (verbose) System.err.println("Ignoring ACK from unexpected endpoint " + source);
                        continue;
                    }
                    Endpoint active = activeSenderEndpoint.get();
                    if (active != null) {
                        decideAndEnqueue("ack", wireBytes, parseAckNum(msg));
                    }
                }
            }
        } catch (SocketException e) {
            if (running) {
                if (verbose) System.err.println("Receiver receive error: " + e.getMessage());
            }
        } catch (IOException e) {
            if (verbose) System.err.println("Receiver IO error: " + e.getMessage());
        }
    }

    private void decideAndEnqueue(String direction, byte[] wireBytes, Integer parsedNumber) {
        long ordinal;
        if (direction.equals("data")) {
            ordinal = dataOrdinal.getAndIncrement();
        } else {
            ordinal = ackOrdinal.getAndIncrement();
        }

        if (parsedNumber != null) {
            Set<Integer> dropSet = direction.equals("data") ? dropDataOnce : dropAckOnce;
            if (dropSet.remove(parsedNumber)) {
                logDecision(direction, parsedNumber, ordinal, "scripted", 0);
                return;
            }
        }

        int lossPercent = direction.equals("data") ? dataLossPercent : ackLossPercent;
        if (direction.equals("data") ? dataRandom.nextInt(100) < lossPercent : ackRandom.nextInt(100) < lossPercent) {
            logDecision(direction, parsedNumber, ordinal, "random", 0);
            return;
        }

        long receiveTime = System.nanoTime();
        long baseDelayNanos = delayMs * 1_000_000L;
        long jitterNanos = 0;
        if (jitterMs > 0) {
            jitterNanos = (direction.equals("data") ? dataRandom.nextInt((int) (jitterMs + 1)) : ackRandom.nextInt((int) (jitterMs + 1))) * 1_000_000L;
        }
        long eligible = receiveTime + baseDelayNanos + jitterNanos;

        if (direction.equals("data") ? dataRandom.nextInt(100) < reorderPercent : ackRandom.nextInt(100) < reorderPercent) {
            eligible += reorderExtraMs * 1_000_000L;
        }

        Endpoint destination = direction.equals("data") ? receiverEndpoint.get() : activeSenderEndpoint.get();
        if (destination != null) {
            DeliveryItem item = new DeliveryItem(direction, ordinal, wireBytes, destination,
                    parsedNumber, eligible, eligible - receiveTime);
            if (direction.equals("data")) {
                dataQueue.offer(item);
            } else {
                ackQueue.offer(item);
            }
        }
    }

    private Integer parseDataSeqnum(String msg) {
        String[] parts = msg.split(" ");
        if (parts.length >= 3) {
            try {
                return Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Integer parseAckNum(String msg) {
        String[] parts = msg.split(" ");
        if (parts.length >= 3) {
            try {
                return Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private void forwardControl(byte[] wireBytes) {
        Endpoint ep = receiverEndpoint.get();
        if (ep == null) {
            if (verbose) System.err.println("Cannot forward control: no receiver endpoint");
            return;
        }
        try {
            DatagramPacket packet = new DatagramPacket(wireBytes, wireBytes.length, ep.address, ep.port);
            receiverSocket.send(packet);
        } catch (IOException e) {
            if (verbose) System.err.println("Control forward error: " + e.getMessage());
        }
    }

    private void runDelivery(String direction, DelayQueue<DeliveryItem> queue) {
        try {
            while (running) {
                DeliveryItem item = queue.take();
                long now = System.nanoTime();

                long bandwidthBitsPerSecond = bandwidthKbps * 1000L;
                long serializationNanos = bandwidthKbps == 0 ? 0 :
                        (long) Math.ceil(item.wireBytes.length * 8 * 1_000_000_000L / (double) bandwidthBitsPerSecond);
                long serializationStart = Math.max(now, direction.equals("data") ? directionNextAvailableData : directionNextAvailableAck);
                long serializationFinish = serializationStart + serializationNanos;

                if (direction.equals("data")) {
                    directionNextAvailableData = serializationFinish;
                } else {
                    directionNextAvailableAck = serializationFinish;
                }

                sleepUntil(serializationFinish);

                try {
                    DatagramPacket packet = new DatagramPacket(item.wireBytes, item.wireBytes.length, item.destination.address, item.destination.port);
                    if (direction.equals("data")) {
                        receiverSocket.send(packet);
                    } else {
                        clientSocket.send(packet);
                    }
                    logDecision(direction, item.number, item.ordinal, "forwarded", item.impairmentDelayNanos);
                } catch (IOException e) {
                    if (verbose) System.err.println(direction + " delivery error: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (verbose) System.err.println(direction + " delivery exception: " + e.getMessage());
        }
    }

    private static void sleepUntil(long deadlineNanos) throws InterruptedException {
        long waitNanos;
        while ((waitNanos = deadlineNanos - System.nanoTime()) > 0) {
            Thread.sleep(waitNanos / 1_000_000L, (int) (waitNanos % 1_000_000L));
        }
    }

    private void logDecision(String direction, Integer number, long ordinal, String reason, long delayNanos) {
        if (verbose) {
            String field = direction.equals("data") ? "seq" : "ack";
            String numStr = number != null ? " " + field + "=" + number : "";
            if (reason.equals("forwarded")) {
                System.err.printf(Locale.ROOT,
                        "FORWARD direction=%s%s ordinal=%d delay_ms=%.3f%n",
                        direction, numStr, ordinal, delayNanos / 1_000_000.0);
            } else {
                System.err.println("DROP direction=" + direction + numStr + " ordinal=" + ordinal + " reason=" + reason);
            }
        }
    }

    private record Args(int listenPort, String receiverHost, int receiverPort,
            long seed, int dataLoss, int ackLoss, long delayMs, long jitterMs,
            int reorderPercent, long reorderExtraMs, long bandwidthKbps,
            List<Integer> dropDataOnce, List<Integer> dropAckOnce, boolean verbose) {
    }

    private static class Endpoint {
        final InetAddress address;
        final int port;

        Endpoint(String host, int port) {
            try {
                this.address = InetAddress.getByName(host);
                this.port = port;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid host: " + host);
            }
        }

        Endpoint(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Endpoint)) return false;
            Endpoint e = (Endpoint) o;
            return address.equals(e.address) && port == e.port;
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, port);
        }

        @Override
        public String toString() {
            return address.getHostAddress() + ":" + port;
        }
    }

    private static class DeliveryItem implements Delayed {
        final String direction;
        final long ordinal;
        final byte[] wireBytes;
        final Endpoint destination;
        final Integer number;
        final long eligibleDeliveryNanos;
        final long impairmentDelayNanos;

        DeliveryItem(String direction, long ordinal, byte[] wireBytes, Endpoint destination,
                Integer number, long eligibleDeliveryNanos, long impairmentDelayNanos) {
            this.direction = direction;
            this.ordinal = ordinal;
            this.wireBytes = wireBytes;
            this.destination = destination;
            this.number = number;
            this.eligibleDeliveryNanos = eligibleDeliveryNanos;
            this.impairmentDelayNanos = impairmentDelayNanos;
        }

        static final Comparator<DeliveryItem> COMPARATOR = Comparator
                .comparingLong((DeliveryItem i) -> i.eligibleDeliveryNanos)
                .thenComparingLong(i -> i.ordinal);

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(eligibleDeliveryNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return COMPARATOR.compare(this, (DeliveryItem) other);
        }
    }
}
