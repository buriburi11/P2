# UDP Reliable File Transfer

This project transfers a file from `sender` to `receiver` over UDP, with `proxy`
in the middle. The sender sends file segments only to the proxy; the proxy
forwards them to the receiver and relays acknowledgments back. The sender uses
cumulative ACKs, a bounded sliding window, congestion control, RTT-based timeout
estimation, retransmissions, and fast retransmit to recover from packet loss.

## Requirements

- Java 21 or later (the scripts use Java source-file mode with `--source 21`)
- Bash and the standard `timeout`, `cmp`, and `realpath` utilities to run the
  integration script

No separate compilation or dependency installation is needed.

## Quick test

From the project directory, run:

```bash
bash test-transfer.sh test.txt
```

The script starts a receiver and proxy on local ports, runs the sender, and
checks that the received bytes exactly match the input. On success it prints
`PASS`. It also prints the temporary directory containing logs and the received
file; inspect `sender.log`, `proxy.err`, and `receiver.log` there to see the
transfer behavior.

To exercise recovery from a dropped DATA segment:

```bash
MAX_WINDOW=2 bash test-transfer.sh medium.bin --delay-ms 15 --drop-data-once 1
```

The script accepts an input file followed by any supported proxy options. Its
defaults can be overridden with `MAX_WINDOW`, `DEADLINE`, `RECEIVER_PORT`, and
`PROXY_PORT` environment variables.

## Run the components manually

Open three terminals in the project directory. Use ports that are available on
your machine.

1. Start the receiver, giving it an existing output directory:

   ```bash
   mkdir -p received
   java --source 21 receiver.java 9002 received --verbose
   ```

2. Start the proxy and configure it to forward to the receiver:

   ```bash
   java --source 21 proxy.java \
     --listen-port 9001 \
     --receiver-host 127.0.0.1 \
     --receiver-port 9002 \
     --verbose
   ```

3. Start the sender with the proxy address and the file to transfer:

   ```bash
   java --source 21 sender.java 127.0.0.1 9001 test.txt --verbose
   ```

The receiver writes a completed transfer as `<transfer-id>.received` in the
output directory. The sender prints transfer events and summary statistics.
For a repeatable transfer ID, add `--transfer-id <id>` to the sender command.

## Command-line options

### Sender

```text
java --source 21 sender.java <proxy_host> <proxy_port> <file_path> [options]
```

- `--max-window <segments>`: maximum sender window, from 1 to 32 (default 32)
- `--transfer-id <id>`: use a chosen transfer ID; allowed characters are
  letters, digits, `.`, `_`, and `-` (1 to 64 characters)
- `--verbose`: enable additional diagnostics
- `--help`: print usage

The sender divides the file into segments of up to 1,000 bytes. A file may
contain up to 65,535 segments.

### Proxy

Required options:

- `--listen-port <port>`
- `--receiver-host <host>`
- `--receiver-port <port>`

Optional network simulation options:

- `--data-loss <0..100>` and `--ack-loss <0..100>`: randomly drop DATA or ACK
  packets by percentage
- `--delay-ms <milliseconds>` and `--jitter-ms <milliseconds>`: add delay
- `--reorder-percent <0..100>` and `--reorder-extra-ms <milliseconds>`: reorder
  packets by adding extra delay to selected packets
- `--bandwidth-kbps <rate>`: limit bandwidth
- `--drop-data-once <seq[,seq...]>` and `--drop-ack-once <ack[,ack...]>`: drop
  selected sequence numbers once
- `--seed <number>`: make random network simulation repeatable
- `--verbose`: print proxy diagnostics

## Files

- `sender.java`: reads and segments a file, tracks ACKs and timers, and
  retransmits data as needed
- `proxy.java`: forwards traffic between sender and receiver and can simulate
  packet loss, delay, reordering, and bandwidth limits
- `receiver.java`: reassembles segments, verifies the completed file, and
  writes it to the output directory
- `test-transfer.sh`: starts a local receiver and proxy, runs a transfer, and
  verifies the output against the input
