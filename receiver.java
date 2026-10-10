import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

public class receiver {
    private static final int MAX_SEGMENT_SIZE = 1000;
    private static final int DEFAULT_RECV_BUFFER = 1024 * 1024;
    private static final int MIN_RECV_BUFFER = 128 * 1024;

    private final int listenPort;
    private final Path outputDir;
    private final boolean verbose;
    private final Map<String, Transfer> activeTransfers = new HashMap<>();
    private final Map<String, CompletedTransfer> completedTransfers = new HashMap<>();
    private DatagramSocket socket;
    private volatile boolean running = true;

    public static void main(String[] args) {
        try {
            receiver r = new receiver(parseArgs(args));
            r.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    public receiver(Args args) {
        this.listenPort = args.listenPort;
        this.outputDir = args.outputDir;
        this.verbose = args.verbose;
    }

    private static Args parseArgs(String[] args) throws IllegalArgumentException {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: java receiver.java <listen_port> <output_directory> [--verbose]");
        }

        int listenPort;
        try {
            listenPort = Integer.parseInt(args[0]);
            if (listenPort < 0 || listenPort > 65535) {
                throw new IllegalArgumentException("Invalid port: " + args[0]);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + args[0]);
        }

        Path outputDir = Paths.get(args[1]);
        if (!Files.isDirectory(outputDir)) {
            throw new IllegalArgumentException("Output directory does not exist: " + args[1]);
        }

        boolean verbose = false;
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("--verbose")) {
                verbose = true;
            } else {
                throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }

        return new Args(listenPort, outputDir, verbose);
    }

    public void run() {
        try {
            socket = new DatagramSocket(listenPort);
            socket.setReceiveBufferSize(DEFAULT_RECV_BUFFER);
            int bufferSize = socket.getReceiveBufferSize();
            if (bufferSize < MIN_RECV_BUFFER) {
                throw new IOException("Cannot allocate receive buffer of at least " + MIN_RECV_BUFFER + " bytes");
            }

            System.out.println("RECEIVER_READY port=" + socket.getLocalPort());
            System.out.flush();

            while (running) {
                byte[] buffer = new byte[65535];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String msg = new String(buffer, 0, packet.getLength(), java.nio.charset.StandardCharsets.UTF_8);
                Endpoint source = new Endpoint(packet.getAddress(), packet.getPort());

                if (msg.startsWith("BEGIN ")) {
                    parseAndHandleBegin(msg, source);
                } else if (msg.startsWith("DATA ")) {
                    parseAndHandleData(msg, source);
                } else if (msg.startsWith("END ")) {
                    parseAndHandleEnd(msg, source);
                }
            }
        } catch (SocketException e) {
            if (running) {
                System.err.println("Socket error: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("IO error: " + e.getMessage());
        } finally {
            closeSocket();
        }
    }

    private void closeSocket() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    private void parseAndHandleBegin(String msg, Endpoint source) {
        String[] parts = msg.split(" ", 5);
        if (parts.length != 5) {
            logDiagnostics("Invalid BEGIN message: " + msg);
            return;
        }

        String transferId = parts[1];
        String filename = parts[2];
        int totalSegments;
        try {
            totalSegments = Integer.parseInt(parts[3]);
            if (totalSegments < 0 || totalSegments > 65535) {
                throw new IllegalArgumentException("Invalid total_segments: " + parts[3]);
            }
        } catch (NumberFormatException e) {
            logDiagnostics("Invalid total_segments: " + parts[3]);
            return;
        }

        String expectedChecksum = parts[4];
        if (!expectedChecksum.matches("[a-f0-9]{32}")) {
            logDiagnostics("Invalid checksum format: " + expectedChecksum);
            return;
        }

        handleBegin(transferId, filename, totalSegments, expectedChecksum, source);
    }

    private void parseAndHandleData(String msg, Endpoint source) {
        String[] parts = msg.split(" ", 4);
        if (parts.length < 4) {
            logDiagnostics("Invalid DATA message: " + msg);
            return;
        }

        String transferId = parts[1];
        int seqnum;
        try {
            seqnum = Integer.parseInt(parts[2]);
            if (seqnum < 0) {
                throw new IllegalArgumentException("Invalid seqnum: " + parts[2]);
            }
        } catch (NumberFormatException e) {
            logDiagnostics("Invalid seqnum: " + parts[2]);
            return;
        }

        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(parts[3]);
        } catch (IllegalArgumentException e) {
            logDiagnostics("Invalid Base64 in DATA: " + e.getMessage());
            return;
        }

        int totalSegments = 0;
        Transfer t = activeTransfers.get(transferId);
        if (t != null) {
            totalSegments = t.totalSegments;
        }

        if (payload.length > MAX_SEGMENT_SIZE || (payload.length == 0 && totalSegments > 0)) {
            logDiagnostics("Invalid payload size: " + payload.length);
            return;
        }

        handleData(transferId, seqnum, payload, source);
    }

    private void parseAndHandleEnd(String msg, Endpoint source) {
        if (!msg.equals(msg.trim())) {
            logDiagnostics("Invalid END message (whitespace): " + msg);
            return;
        }
        String[] parts = msg.split(" ", 2);
        if (parts.length != 2) {
            logDiagnostics("Invalid END message: " + msg);
            return;
        }

        String transferId = parts[1];
        handleEnd(transferId, source);
    }

    private void handleBegin(String transferId, String filename, int totalSegments, String expectedChecksum, Endpoint source) {
        Transfer existing = activeTransfers.get(transferId);
        if (existing != null) {
            if (existing.totalSegments != totalSegments || !existing.expectedChecksum.equals(expectedChecksum)) {
                logDiagnostics("Conflicting BEGIN for transfer " + transferId);
                return;
            }
            logDiagnostics("Received duplicate BEGIN for transfer " + transferId);
            return;
        }

        activeTransfers.put(transferId, new Transfer(transferId, filename, totalSegments, expectedChecksum, source));
        logDiagnostics("Received BEGIN for transfer " + transferId);
    }

    private void handleData(String transferId, int seqnum, byte[] payload, Endpoint source) {
        Transfer t = activeTransfers.get(transferId);
        if (t == null) {
            CompletedTransfer ct = completedTransfers.get(transferId);
            if (ct != null) {
                if (!source.equals(ct.source)) {
                    logDiagnostics("DATA from unexpected source for completed transfer " + transferId);
                    return;
                }
                String ackMsg = "ACK " + transferId + " " + ct.cumulativeAck;
                byte[] bytes = ackMsg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ct.source.address, ct.source.port);
                try {
                    socket.send(packet);
                } catch (IOException e) {
                    logDiagnostics("Error sending ACK for completed transfer: " + e.getMessage());
                }
                return;
            }
            logDiagnostics("DATA for unknown transfer " + transferId);
            return;
        }

        if (!source.equals(t.source)) {
            logDiagnostics("DATA from unexpected source for transfer " + transferId);
            return;
        }

        if (seqnum >= t.totalSegments) {
            logDiagnostics("DATA seqnum out of range: " + seqnum);
            return;
        }

        int nextExpected = t.nextExpected;
        if (seqnum == nextExpected) {
            try {
                t.assembledBytes.write(payload);
                t.nextExpected++;
            } catch (IOException e) {
                logDiagnostics("Error writing payload: " + e.getMessage());
                return;
            }
        } else if (seqnum < nextExpected) {
            logDiagnostics("Duplicate DATA seqnum=" + seqnum + " for transfer " + transferId);
        } else {
            logDiagnostics("Future DATA seqnum=" + seqnum + " for transfer " + transferId);
        }

        sendAck(t, source);
    }

    private void handleEnd(String transferId, Endpoint source) {
        Transfer t = activeTransfers.get(transferId);
        if (t == null) {
            CompletedTransfer completed = completedTransfers.get(transferId);
            if (completed != null) {
                if (!source.equals(completed.source)) {
                    logDiagnostics("END from unexpected source for completed transfer " + transferId);
                }
                return;
            }
            logDiagnostics("END for unknown transfer " + transferId);
            return;
        }

        if (!source.equals(t.source)) {
            logDiagnostics("END from unexpected source for transfer " + transferId);
            return;
        }

        if (t.nextExpected != t.totalSegments) {
            logDiagnostics("INCOMPLETE transfer " + transferId + " got END with nextExpected=" + t.nextExpected);
            return;
        }

        completeTransfer(t);
    }

    private void sendAck(Transfer t, Endpoint destination) {
        int cumulativeAck = t.nextExpected - 1;
        String msg = "ACK " + t.transferId + " " + cumulativeAck;
        byte[] bytes = msg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, destination.address, destination.port);
        try {
            socket.send(packet);
        } catch (IOException e) {
            logDiagnostics("Error sending ACK: " + e.getMessage());
        }
    }

    private void completeTransfer(Transfer t) {
        byte[] assembled = t.assembledBytes.toByteArray();
        String actualChecksum = md5Hex(assembled);
        boolean checksumMatch = actualChecksum.equals(t.expectedChecksum);
        String status = checksumMatch ? "OK" : "FAIL";

        if (checksumMatch) {
            String outputPath = outputDir.resolve(t.transferId + ".received").toString();
            Path tempPath = Paths.get(outputPath + ".tmp");
            Path finalPath = Paths.get(outputPath);
            try {
                Files.write(tempPath, assembled);
                Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                logDiagnostics("Error writing output file: " + e.getMessage());
                status = "FAIL";
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException ignored) {}
            }
        }

        String fileField = status.equals("OK") ? outputDir.resolve(t.transferId + ".received").toString() : "none";
        String logMsg = String.format("RECEIVER_COMPLETE transfer=%s segments=%d bytes=%d checksum=%s status=%s file=%s",
                t.transferId, t.totalSegments, assembled.length, actualChecksum, status, fileField);
        System.out.println(logMsg);
        System.out.flush();

        activeTransfers.remove(t.transferId);
        if (checksumMatch && status.equals("OK")) {
            completedTransfers.put(t.transferId,
                    new CompletedTransfer(t.transferId, t.totalSegments - 1, status, t.source));
        }
    }

    private void logDiagnostics(String msg) {
        if (verbose) {
            System.err.println(msg);
        }
    }

    private String md5Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(bytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private record Args(int listenPort, Path outputDir, boolean verbose) {
    }

    private static class Transfer {
        final String transferId;
        final String filename;
        final int totalSegments;
        final String expectedChecksum;
        final Endpoint source;
        final ByteArrayOutputStream assembledBytes = new ByteArrayOutputStream();
        int nextExpected = 0;

        Transfer(String transferId, String filename, int totalSegments, String expectedChecksum, Endpoint source) {
            this.transferId = transferId;
            this.filename = filename;
            this.totalSegments = totalSegments;
            this.expectedChecksum = expectedChecksum;
            this.source = source;
        }
    }

    private static class CompletedTransfer {
        final String transferId;
        final int cumulativeAck;
        final String status;
        final Endpoint source;

        CompletedTransfer(String transferId, int cumulativeAck, String status, Endpoint source) {
            this.transferId = transferId;
            this.cumulativeAck = cumulativeAck;
            this.status = status;
            this.source = source;
        }
    }

    private static class Endpoint {
        final InetAddress address;
        final int port;

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
    }
}
