# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-file custom Reticulum (RNS) interface, [Interface/Meshtastic_Interface.py](Interface/Meshtastic_Interface.py), that tunnels RNS packets over Meshtastic radios. It is not a package: users copy the file into `~/.reticulum/interfaces/` and add a `[[Meshtastic Interface]]` block (`type = Meshtastic_Interface`) to their Reticulum config. See README.md for the config keys (`port` / `ble_port` / `tcp_port`, `data_speed`, `channel_index`, `hop_limit`, `bitrate`, `fragment_size`, `announce_max_hops`).

There is no build, lint config, or test framework. Dependencies: `rns`, `meshtastic` (recent enough to have `portnums_pb2.RETICULUM_TUNNEL_APP`), `pypubsub`.

## Testing

The only test is a fragmentation round-trip script that prints `True` on success. It imports the module by name, so run it from inside `Interface/` (RNS must be installed because the module imports `RNS.Interfaces.Interface` at top level):

```
cd Interface && python packet_handling_test.py
```

Anything beyond `PacketHandler` requires real Meshtastic hardware and a running Reticulum instance.

## Architecture

- **Loaded by RNS, not imported.** Reticulum exec's the file and picks up the module-level `interface_class`. `RNS` is never imported in the file — `RNS.log` / `RNS.panic` resolve from the globals RNS injects at load time. Don't "fix" this by assuming the file runs standalone; only `PacketHandler`/`calc_index` are usable outside RNS.
- **Event-driven I/O via pubsub.** The meshtastic library publishes `meshtastic.receive`, `meshtastic.connection.established`, and `meshtastic.connection.lost`; these drive `process_message`, `connection_complete` (which calls `configure_device`), and `connection_closed` (hands off to a `reconnect_loop` thread — never block the callback, it runs on meshtastic's single publishing thread; the loop first lets the library recover a still-open port itself, then closes the old interface and retries `open_interface` every `reconnect_delay` s). If the radio can't be opened at startup, `__init__` logs it and starts `reconnect_loop` instead of raising (only a missing port setting raises). A `watchdog_loop` thread forces the same reconnect whenever the interface has been down for `watchdog_timeout` s, covering failures the events miss (errors in pubsub callbacks are swallowed by the library). `close_interface` exists because the library's `close()` can hang or raise before releasing the exclusively-locked serial port. `configure_device` writes the LoRa `modem_preset` if it differs from `data_speed` (the radio reboots and reconnects); only when the preset already matches does it set `online = True` and start the single `write_loop` thread (which idles while `online` is False).
- **Outbound path.** `process_outgoing` (called by RNS Transport) wraps each RNS packet in a `PacketHandler`, which splits it into fragments of at most `fragment_size` bytes (default 200) each prefixed with a 2-byte `struct 'Bb'` header: packet index (0–255, wraps via `calc_index`) and position (1-based; the **last fragment's position is negated** to mark end-of-packet). `(index, pos)` tuples go into `packet_i_queue`; handlers live in `outgoing_packet_storage[index]`. `write_loop` drains the queue at a fixed rate from `speed_to_delay[data_speed]` and calls `interface.sendData` on `RETICULUM_TUNNEL_APP` with `wantAck=False`.
- **Inbound path and loss recovery.** `process_message` keeps per-sender state keyed by Meshtastic node id: `assembly_dict` (partial `PacketHandler`s by index), `expected_index`, and `requested_index`. When an unexpected fragment arrives, it assumes the expected one was lost and queues a retransmit request — a payload of `b'REQ' + header` — at the front of the queue via the special storage key `-1`. A received `REQ` puts the requested `(index, pos)` at the front of the sender's own queue (this only works while that handler is still in `outgoing_packet_storage`, which is overwritten when the index wraps).
- **Broadcast only, by design.** Every fragment goes to `BROADCAST_ADDR` on `channel_index`. Upstream had a never-working unicast path (a dest-hash→node cache with an `is PacketHandler` check); it was removed deliberately — don't reintroduce unicast. Meshtastic DMs cost the same airtime, and broadcast is what the ScotMesh/cheirekov Android bridge `broadcast` mode interoperates with. Inbound DMs are still accepted (they bypass the channel filter, since PKI DMs report channel 0).
- `announce_max_hops` filtering in `process_outgoing` parses the raw RNS header (hops byte, context byte); it can't read packets once IFAC has masked them.
- `HW_MTU = 564` with 200-byte fragments means a packet is at most 3 fragments (4 at 155, 18 at the 32-byte minimum); the signed-byte position field caps a packet at 127 fragments.
