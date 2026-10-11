import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** One file per invocation; DATA travels through the proxy, never directly to the receiver. */
public class sender {
    private static final double ALPHA = 0.125;
    private static final double BETA = 0.25;
    private static final double INITIAL_RTO_MS = 1000.0;
    private static final double MIN_RTO_MS = 100.0;
    private static final double MAX_RTO_MS = 60000.0;
    private static final int DEFAULT_MAX_WINDOW = 32;
    private static final int SEGMENT_SIZE = 1000;
    private static final int MAX_TOTAL_BYTES = 65_535 * SEGMENT_SIZE;

    private final Config config;
    private final byte[] fileBytes;
    private final List<Segment> segments;
    private final int totalSegments;
    private final DatagramSocket socket;
    private final InetSocketAddress proxyAddress;
    private final Object stateLock = new Object();

    // All mutable fields below, including Segment timing fields, are guarded by stateLock.
    private int base;                   // Oldest unacknowledged segment
    private int nextSeqNum;             // First segment never sent
    private int cwnd;                   // Congestion window, in whole segments
    private int ssthresh;
    private int caAckCredits;
    private int duplicateAckCount;
    private int fastRetransmittedBase;
    private boolean timerRunning;
    private long timerStartedNanos;
    private Double estimatedRttMs;
    private Double deviationMs;
    private double rtoMs;
    private boolean running;
    private Throwable fatalError;

    private int originalSegmentsSent;
    private int retransmissions;
    private int duplicateAcks;
    private int timeouts;
    private int validRttSamples;
    private double rttSampleSumMs;

    public static void main(String[] args) {
        try {
            Config config = parseArgs(args);
            new sender(config).run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private sender(Config config) throws IOException {
        this.config = config;
        this.fileBytes = Files.readAllBytes(Path.of(config.filePath()));
        this.segments = prepareSegments(fileBytes);
        this.totalSegments = segments.size();
        this.proxyAddress = new InetSocketAddress(config.proxyHost(), config.proxyPort());
        this.socket = new DatagramSocket();
        this.base = 0;
        this.nextSeqNum = 0;
        this.cwnd = 1;
        this.ssthresh = 64;
        this.caAckCredits = 0;
        this.duplicateAckCount = 0;
        this.fastRetransmittedBase = -1;
        this.timerRunning = false;
        this.estimatedRttMs = null;
        this.deviationMs = null;
        this.rtoMs = INITIAL_RTO_MS;
        this.running = true;
    }

    private static Config parseArgs(String[] args) {
        if (args.length == 1 && args[0].equals("--help")) {
            usage();
            System.exit(0);
        }
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: java sender.java <proxy_host> <proxy_port> <file_path> [options]");
        }

        int proxyPort;
        try {
            proxyPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + args[1]);
        }
        if (proxyPort < 1 || proxyPort > 65535) {
            throw new IllegalArgumentException("Invalid port: " + args[1]);
        }

        int maxWindow = DEFAULT_MAX_WINDOW;
        String transferId = null;
        boolean verbose = false;
        for (int i = 3; i < args.length; i++) {
            switch (args[i]) {
                case "--max-window":
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Missing value for --max-window");
                    try {
                        maxWindow = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid max-window: " + args[i]);
                    }
                    if (maxWindow < 1 || maxWindow > 32) {
                        throw new IllegalArgumentException("max-window must be between 1 and 32");
                    }
                    break;
                case "--transfer-id":
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Missing value for --transfer-id");
                    transferId = args[++i];
                    if (!transferId.matches("[A-Za-z0-9._-]{1,64}")) {
                        throw new IllegalArgumentException("Invalid transfer-id: " + transferId);
                    }
                    break;
                case "--verbose":
                    verbose = true;
                    break;
                case "--help":
                    usage();
                    System.exit(0);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }

        Path file = Path.of(args[2]);
        if (!Files.isRegularFile(file)) throw new IllegalArgumentException("Not a regular file: " + file);
        try {
            if (Files.size(file) > MAX_TOTAL_BYTES) throw new IllegalArgumentException("File too large: " + file);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read file size: " + file, e);
        }
        if (transferId == null) transferId = "xfer-" + Long.toUnsignedString(System.nanoTime());
        return new Config(args[0], proxyPort, args[2], maxWindow, transferId, verbose);
    }

    private static void usage() {
        System.out.println("Usage: java sender.java <proxy_host> <proxy_port> <file_path> [options]");
        System.out.println("  --max-window <segments>   Default 32; range 1..32");
        System.out.println("  --transfer-id <id>        Use a fixed ID instead of a generated one");
        System.out.println("  --verbose                 Print diagnostics on standard error");
        System.out.println("  --help                    Print this help");
    }

    private List<Segment> prepareSegments(byte[] bytes) {
        List<Segment> result = new ArrayList<>();
        for (int offset = 0; offset < bytes.length; offset += SEGMENT_SIZE) {
            int end = Math.min(offset + SEGMENT_SIZE, bytes.length);
            byte[] chunk = Arrays.copyOfRange(bytes, offset, end);
            String message = "DATA " + config.transferId() + " " + result.size() + " "
                    + Base64.getEncoder().encodeToString(chunk);
            result.add(new Segment(result.size(), chunk.length, message.getBytes(StandardCharsets.UTF_8)));
        }
        return result;
    }

    private static String sanitizeFilename(String name) {
        String safe = name.replaceAll("[^A-Za-z0-9._-]", "_");
        return safe.substring(0, Math.min(128, safe.length()));
    }

    private static String md5Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(bytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void sendControl(String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(bytes, bytes.length, proxyAddress));
    }

    private void run() throws IOException {
        long startNanos = System.nanoTime();
        Thread ackReader = null;
        try {
            String basename = Path.of(config.filePath()).getFileName().toString();
            sendControl("BEGIN " + config.transferId() + " " + sanitizeFilename(basename)
                    + " " + totalSegments + " " + md5Hex(fileBytes));

            if (totalSegments > 0) {
                ackReader = new Thread(this::runAckReader, "sender-ack-reader");
                ackReader.start();
                synchronized (stateLock) {
                    logCwnd();
                }
                runSendLoop();
            }

            sendControl("END " + config.transferId());
            shutdownAckReader(ackReader);
            ackReader = null;
            synchronized (stateLock) {
                double elapsedMs = millis(System.nanoTime() - startNanos);
                logEvent("COMPLETE segments=%d retransmissions=%d time=%.3f",
                        originalSegmentsSent, retransmissions, elapsedMs);
                double average = validRttSamples == 0 ? 0.0 : rttSampleSumMs / validRttSamples;
                logEvent("STATS segments_sent=%d retransmissions=%d dup_acks=%d timeouts=%d final_cwnd=%d avg_rtt=%.3f total_time=%.3f",
                        originalSegmentsSent, retransmissions, duplicateAcks, timeouts, cwnd, average, elapsedMs);
            }
        } finally {
            shutdownAckReader(ackReader);
        }
    }

    /** Main thread. Locking and sleep placement are supplied; send/timer decisions are not. */
    private void runSendLoop() {
        while (true) {
            long sleepMillis;
            synchronized (stateLock) {
                if (fatalError != null) throw new IllegalStateException("ACK reader failed", fatalError);
                if (base == totalSegments) return;
                sleepMillis = sendStep(System.nanoTime());
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Sender interrupted", e);
            }
        }
    }

    /** Called with stateLock held; return a sleep of 1..10 ms until the next check. */
    private long sendStep(long now) {
        fillWindow(now);

        if (!timerRunning) {
            return 10;
        }

        double ms = millis(now - timerStartedNanos);

        if (ms >= rtoMs) {
            handleTimeout(now);
            return 1;
        }

        long remaining = (long) Math.ceil(rtoMs - ms);
        return Math.max(1, Math.min(10, remaining));
    }

    /** Called with stateLock held. A first send advances nextSeqNum exactly once. */
    private void fillWindow(long now) {
        // Flights are the data segments that have been sent but not yet acknowledged.
        boolean flightEmpty = base == nextSeqNum;

        int effectiveWindow = Math.min(cwnd, config.maxWindow());
        int limit = Math.min(totalSegments, base + effectiveWindow);

        // Logic to send segments.
        // Basically, if the next sequence number is less than the limit, we send it to the receiver.
        // Then, the next sequence number is incremented.
        while (nextSeqNum < limit) {
            int seq = nextSeqNum;
            transmitFirst(seq, now);
            nextSeqNum++;
        }

        // Timer logic.
        // If the flight is empty and there are segments to send, start the timer.
        if (flightEmpty && base < nextSeqNum) {
            timerRunning = true;
            timerStartedNanos = now;
        }
    }

    /** Provided I/O only. Called with stateLock held; caller advances nextSeqNum. */
    private void transmitFirst(int seq, long now) {
        Segment segment = segments.get(seq);
        try {
            socket.send(new DatagramPacket(segment.wireBytes, segment.wireBytes.length, proxyAddress));
        } catch (IOException e) {
            throw new UncheckedIOException("DATA send failed", e);
        }
        segment.lastSentNanos = now;
        originalSegmentsSent++;
        logEvent("SEND seq=%d size=%d", seq, segment.rawSize);
    }

    /** Provided I/O only. Called with stateLock held; reason is timeout or fast_retransmit. */
    private void retransmit(int seq, String reason, long now) {
        Segment segment = segments.get(seq);
        try {
            socket.send(new DatagramPacket(segment.wireBytes, segment.wireBytes.length, proxyAddress));
        } catch (IOException e) {
            throw new UncheckedIOException("DATA resend failed", e);
        }
        segment.lastSentNanos = now;
        segment.retransmitted = true; // Sticky: an ACK can no longer identify which copy arrived.
        retransmissions++;
        logEvent("RESEND seq=%d reason=%s", seq, reason);
    }

    /** Called with stateLock held. Raw ACK classification is student work. */
    private void handleAck(int ack, long now) {
        if (base <= ack && ack < nextSeqNum) {
            handleAdvancingAck(ack, now);
        } else if (ack == base - 1 && base < nextSeqNum) {
            handleDuplicateAck(ack, now);
        }
    }

    /** Called with stateLock held for a valid advancing cumulative ACK. */
    private void handleAdvancingAck(int ack, long now) {
        int oldBase = base;
        int newAck = ack - oldBase + 1;
        int oldcwnd = cwnd;

        Segment ackSegment = segments.get(ack);
        Double sampleMS = null;

        if (!ackSegment.retransmitted) {
            sampleMS = millis(now - ackSegment.lastSentNanos);
            updateRtt(sampleMS);
        }

        base = ack + 1;
        duplicateAckCount = 0;
        fastRetransmittedBase = -1;

        applyAckCredits(newAck);

        if (base < nextSeqNum) {
            timerRunning = true;
            timerStartedNanos = now;
        } else {
            timerRunning = false;
        }

        logAck(ack, sampleMS);

        if (sampleMS != null) {
            logRtt(sampleMS);
        }

        if (cwnd != oldcwnd) {
            logCwnd();
        }
    }

    /** Called with stateLock held for a qualifying duplicate ACK. */
    private void handleDuplicateAck(int ack, long now) {
        duplicateAckCount++;
        duplicateAcks++;

        logDuplicateAck(ack);

        if (duplicateAckCount == 3 && fastRetransmittedBase != base) {
            int oldCwnd = cwnd;

            logFastRetransmit();

            ssthresh = Math.max(oldCwnd / 2, 2);
            cwnd = Math.min(ssthresh, config.maxWindow());
            caAckCredits = 0;
            fastRetransmittedBase = base;

            logCwnd();
            retransmit(base, "fast_retransmit", now);

            timerRunning = true;
            timerStartedNanos = now;
        }
    }

    /** Called with stateLock held when the DATA timer expires. */
    private void handleTimeout(long now) {
        int outstanding = nextSeqNum - base;
        double rTo = rtoMs;
        int oldCwnd = cwnd;

        rtoMs = Math.min(MAX_RTO_MS, 2 * rTo);

        ssthresh = Math.max(oldCwnd / 2, 2);
        cwnd = 1;
        caAckCredits = 0;
        duplicateAckCount = 0;

        timeouts++;

        logTimeout(outstanding, rTo);
        logCwnd();

        for (int seq = base; seq < nextSeqNum; seq++) {
            retransmit(seq, "timeout", now);
        }

        timerRunning = true;
        timerStartedNanos = now;
    }

    /** Called with stateLock held; count is the number of newly acknowledged segments. */
    private void applyAckCredits(int count) {
        int maxWindow = config.maxWindow();

        while (count > 0 && cwnd < maxWindow) {
            if (cwnd < ssthresh) {
                cwnd++;
            } else {
                caAckCredits++;
            }

            if (caAckCredits >= cwnd) {
                caAckCredits -= cwnd;
                cwnd++;
            }

            count--;
        }

        if (cwnd >= maxWindow) {
            cwnd = maxWindow;
            caAckCredits = 0;
        }
    }

    /** Called with stateLock held for one Karn-eligible RTT sample (milliseconds). */
    private void updateRtt(double sampleMs) {
        if (estimatedRttMs == null) {
            estimatedRttMs = sampleMs;
            deviationMs = sampleMs / 2.0;
        } else {
            double oldEst = estimatedRttMs;
            double oldDeviation = deviationMs;
            double err = Math.abs(sampleMs - oldEst);

            deviationMs = (1.0 - BETA) * oldDeviation + BETA * err;

            estimatedRttMs = (1.0 - ALPHA) * oldEst + ALPHA * sampleMs;
        }

        double calculatedRto = estimatedRttMs + 4 * deviationMs;

        rtoMs = Math.max(MIN_RTO_MS, Math.min(MAX_RTO_MS, calculatedRto));

        validRttSamples++;
        rttSampleSumMs += sampleMs;
    }

    private void runAckReader() {
        byte[] buffer = new byte[65535];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            socket.setSoTimeout(100);
            while (true) {
                synchronized (stateLock) {
                    if (!running) return;
                }
                packet.setLength(buffer.length);
                try {
                    socket.receive(packet);
                } catch (SocketTimeoutException e) {
                    continue; // Receive polling is not a DATA retransmission timeout.
                }

                if (!packet.getAddress().equals(proxyAddress.getAddress())
                        || packet.getPort() != proxyAddress.getPort()) continue;
                String msg = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                String prefix = "ACK " + config.transferId() + " ";
                if (!msg.startsWith(prefix) || !msg.matches("ACK [A-Za-z0-9._-]{1,64} -?[0-9]+")) continue;
                int ack;
                try {
                    ack = Integer.parseInt(msg.substring(prefix.length()));
                } catch (NumberFormatException e) {
                    continue;
                }
                synchronized (stateLock) {
                    if (!running) return;
                    handleAck(ack, System.nanoTime());
                }
            }
        } catch (IOException | RuntimeException e) {
            synchronized (stateLock) {
                if (running) fatalError = e;
            }
        }
    }

    private void shutdownAckReader(Thread reader) {
        synchronized (stateLock) {
            running = false;
        }
        socket.close();
        if (reader != null) {
            try {
                reader.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Supplied event formats. Students call these at the appropriate state transitions.
    private void logAck(int ack, Double sampleMs) {
        if (sampleMs == null) logEvent("ACK ack=%d", ack);
        else logEvent("ACK ack=%d rtt=%.3f", ack, sampleMs);
    }

    private void logRtt(double sampleMs) {
        logEvent("RTT sample=%.3f estimated=%.3f deviation=%.3f timeout=%.3f",
                sampleMs, estimatedRttMs, deviationMs, rtoMs);
    }

    private void logCwnd() {
        logEvent("CWND cwnd=%d ssthresh=%d phase=%s", cwnd, ssthresh,
                cwnd < ssthresh ? "slow_start" : "congestion_avoidance");
    }

    private void logTimeout(int outstanding, double rtoBefore) {
        logEvent("TIMEOUT base=%d resending=%d rto_before=%.3f rto_after=%.3f",
                base, outstanding, rtoBefore, rtoMs);
    }

    private void logDuplicateAck(int ack) {
        logEvent("DUP_ACK ack=%d count=%d", ack, duplicateAckCount);
    }

    private void logFastRetransmit() {
        logEvent("FAST_RETRANSMIT seq=%d", base);
    }

    /** Caller holds stateLock for any protocol event; empty-file events have no ACK thread. */
    private void logEvent(String format, Object... args) {
        System.out.printf(Locale.ROOT, format + "%n", args);
        System.out.flush();
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static UnsupportedOperationException incomplete(String method) {
        return new UnsupportedOperationException("TODO: implement " + method);
    }

    private record Config(String proxyHost, int proxyPort, String filePath,
                          int maxWindow, String transferId, boolean verbose) {}

    private static class Segment {
        final int seq;
        final int rawSize;
        final byte[] wireBytes;
        long lastSentNanos;
        boolean retransmitted;

        Segment(int seq, int rawSize, byte[] wireBytes) {
            this.seq = seq;
            this.rawSize = rawSize;
            this.wireBytes = wireBytes;
        }
    }
}
