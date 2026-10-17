import datetime
import errno
import json
import logging
from dataclasses import dataclass
from typing import Any

import zmq
from pydantic import ValidationError

from pqn_hardware.network.packet import PROTOCOL_VERSION
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import ErrorCode
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import ProvidersPayload
from pqn_hardware.network.packet import ProviderSummary
from pqn_hardware.network.packet import RegisterPayload
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode

logger = logging.getLogger(__name__)

# ZMQ transport heartbeat defaults: how often the Router pings its peers and
# how long without traffic it waits before dropping a half-open connection.
ZMQ_HEARTBEAT_IVL = 1000
ZMQ_HEARTBEAT_TIMEOUT = 5000


@dataclass(frozen=True)
class _DirectoryEntry:
    """One Provider's record in the Directory: its Last seen time and the Instruments it reported."""

    last_seen: datetime.datetime
    devices: dict[str, DeviceSummary]


def _readable_reply_to(data: bytes) -> str | None:
    """Return the id of undecodable wire bytes when they parse as a JSON object with a string id."""
    try:
        parsed = json.loads(data)
    except ValueError:
        return None
    packet_id: Any = parsed.get("id") if isinstance(parsed, dict) else None
    if isinstance(packet_id, str):
        return packet_id
    return None


class Router:
    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 5555,
        heartbeat_ivl: int = ZMQ_HEARTBEAT_IVL,
        heartbeat_timeout: int = ZMQ_HEARTBEAT_TIMEOUT,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.heartbeat_ivl = heartbeat_ivl
        self.heartbeat_timeout = heartbeat_timeout

        # TODO: Verify that this address is valid
        self.address = f"tcp://{host}:{port}"

        # The Router's record of registered Providers: name, Last seen and devices (ADR-0003).
        # It is used for diagnostics and GET_PROVIDERS, never for delivery, and entries are
        # never removed while the Router runs.
        self.directory: dict[str, _DirectoryEntry] = {}

        # Per Client identity, the latest request-id frame its REQ socket sent (REQ_CORRELATE)
        # and the id of that request Packet. A reply whose reply_to matches that id is sent
        # back with the request-id frame; any other Packet to the Client goes without it, so
        # the Client's socket drops it.
        self.request_ids: dict[bytes, tuple[bytes, str]] = {}

        self.context: zmq.Context[zmq.Socket[bytes]] | None = None
        self.socket: zmq.Socket[bytes] | None = None
        self.running = False

    def start(self) -> None:
        logger.info("Starting router %s at %s", self.name, self.address)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        # Let a reconnecting peer reclaim its routing id. Without this, libzmq quarantines a
        # peer whose identity is already taken (e.g. by a half-open socket left behind when a
        # provider drops off the network) and silently discards everything it sends.
        self.socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
        # Deliver by identity: sending to a peer that is not connected raises instead of
        # silently queuing the Packet, and the transport heartbeats close half-open
        # connections in seconds (ADR-0003).
        self.socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.socket.setsockopt(zmq.HEARTBEAT_IVL, self.heartbeat_ivl)
        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, self.heartbeat_timeout)
        self.socket.bind(self.address)
        logger.info("Router %s is now listening on %s", self.name, self.address)
        self.running = True

        try:
            while self.running:
                identity_binary, packet = self.listen()
                if packet is None or identity_binary is None:
                    logger.error("Error listening to packets. Either the packet is None or the identity is None.")
                    continue

                try:
                    if packet.intent == PacketIntent.ERROR and packet.destination == self.name:
                        # An ERROR Packet is itself a reply and is never answered, so errors
                        # cannot bounce forever.
                        logger.warning("Received an ERROR Packet, which the Router never asked for: %s", packet)
                    elif packet.request == Request.REGISTER:
                        self.handle_registration(identity_binary, packet)
                    else:
                        self.handle_pass_packet(identity_binary, packet)
                except ValidationError as e:
                    # A Payload that fails validation (e.g. an invalid REGISTER Payload) gets a
                    # fast MALFORMED_PACKET reply instead of crashing the Router.
                    self.handle_packet_error(
                        identity_binary,
                        ErrorCode.MALFORMED_PACKET,
                        f"Payload does not match request '{packet.request}': {e}",
                        reply_to=packet.id,
                    )
                except zmq.error.ZMQError as e:
                    # A reply the Router cannot send (its peer already went away) is logged and
                    # dropped: the Router never crashes on its own replies.
                    logger.warning("Could not send a reply to %s: %s", identity_binary, e)

        finally:
            self.socket.close()

    def handle_registration(self, identity_binary: bytes, packet: Packet) -> None:
        """Handle a REGISTER Packet: update the Directory and ACK."""
        if packet.destination != self.name:
            self.handle_packet_error(
                identity_binary,
                ErrorCode.MALFORMED_PACKET,
                f"Router {self.name} is not the destination",
                reply_to=packet.id,
            )
            return
        payload = RegisterPayload.model_validate(packet.payload)
        match payload.kind:
            case NetworkElementClass.PROVIDER:
                # The Directory holds Providers only. A Heartbeat (a repeated REGISTER)
                # refreshes Last seen and devices; entries are never removed.
                self.directory[packet.source] = _DirectoryEntry(
                    last_seen=datetime.datetime.now(tz=datetime.UTC), devices=payload.devices
                )
                logger.info("InstrumentProvider %s registered", packet.source)
            case NetworkElementClass.CLIENT:
                logger.info("Client %s registered", packet.source)

        # The identity frame may not be valid UTF-8 (e.g. libzmq's auto identities); the raw
        # bytes stay the routing frame, the Packet only needs a readable text destination.
        ack_packet = Packet(
            intent=PacketIntent.REGISTRATION_ACK,
            request=Request.ACKNOWLEDGE,
            source=self.name,
            destination=identity_binary.decode("utf-8", errors="replace"),
            reply_to=packet.id,
        )
        self._send(identity_binary, ack_packet)

    def handle_pass_packet(self, identity_binary: bytes, packet: Packet) -> None:
        """Handle all the logic to get a packet from one place to another."""
        if packet.destination == self.name:
            self._handle_packet_to_self(identity_binary, packet)
            return

        # Deliver by identity, never by the Directory (ADR-0003).
        dest = packet.destination.encode()
        forward_packet = packet.model_copy(update={"hops": packet.hops + 1})
        logger.info("Routing packet to %s", packet.destination)
        try:
            self._send(dest, forward_packet)
        except zmq.error.ZMQError as e:
            if e.errno not in (errno.EHOSTUNREACH, errno.EAGAIN):
                raise
            self._handle_undeliverable(identity_binary, packet, e)

    def _handle_packet_to_self(self, identity_binary: bytes, packet: Packet) -> None:
        """Answer a request addressed to the Router itself."""
        match packet.request:
            case Request.PING:
                pong_packet = Packet(
                    intent=PacketIntent.PING,
                    request=Request.PONG,
                    source=self.name,
                    destination=packet.source,
                    reply_to=packet.id,
                )
                self._send(identity_binary, pong_packet)
            case Request.GET_PROVIDERS:
                self._handle_get_providers(identity_binary, packet)
            case _:
                self.handle_packet_error(
                    identity_binary,
                    ErrorCode.UNSUPPORTED_REQUEST,
                    f"Router {self.name} does not handle request '{packet.request}' addressed to itself.",
                    reply_to=packet.id,
                )

    def _handle_get_providers(self, identity_binary: bytes, packet: Packet) -> None:
        """Answer a GET_PROVIDERS request with the Directory."""
        providers = [
            ProviderSummary(name=name, last_seen=entry.last_seen, devices=entry.devices)
            for name, entry in self.directory.items()
        ]
        reply_packet = Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_PROVIDERS,
            source=self.name,
            destination=packet.source,
            reply_to=packet.id,
            payload=ProvidersPayload(providers=providers).model_dump(),
        )
        self._send(identity_binary, reply_packet)

    def _handle_undeliverable(self, identity_binary: bytes, packet: Packet, error: zmq.error.ZMQError) -> None:
        """Reply to a Packet whose destination cannot be reached."""
        if packet.intent == PacketIntent.ERROR:
            # An ERROR Packet is itself a reply and is never answered; the Router drops it and
            # logs a warning naming the destination and the error code it carried.
            carried_code = packet.payload.get("code") if packet.payload is not None else None
            logger.warning(
                "Cannot deliver the ERROR Packet (code %s) to '%s': %s", carried_code, packet.destination, error
            )
            return
        if packet.destination in self.directory:
            entry = self.directory[packet.destination]
            self.handle_packet_error(
                identity_binary,
                ErrorCode.DESTINATION_UNREACHABLE,
                f"Destination '{packet.destination}' cannot be reached: {error}",
                reply_to=packet.id,
                details={"last_seen": entry.last_seen.isoformat()},
            )
            return
        self.handle_packet_error(
            identity_binary,
            ErrorCode.UNKNOWN_DESTINATION,
            f"Destination '{packet.destination}' is not registered with {self.name}. "
            f"Registered: {', '.join(self.directory)}",
            reply_to=packet.id,
        )

    def listen(self) -> tuple[bytes, Packet] | tuple[None, None]:
        # This should never happen, but mypy complains if the check is not done
        if self.socket is None:
            msg = "Socket is None, cannot listen."
            logger.error(msg)
            raise RuntimeError(msg)

        # Depending on who is sending a request, the number of items received will be different.
        # DEALER sockets send 2 items, REQ sockets send an empty delimiter, and a REQ socket with
        # REQ_CORRELATE also sends its request-id frame.
        dealer_parts = 2
        req_parts = 3
        correlated_parts = 4
        request_id: bytes | None = None
        request = self.socket.recv_multipart()
        if len(request) == dealer_parts:
            identity_binary, data = request
        elif len(request) == req_parts:
            identity_binary, _, data = request
        elif len(request) == correlated_parts:
            identity_binary, request_id, _, data = request
        else:
            self.handle_packet_error(
                request[0], ErrorCode.MALFORMED_PACKET, f"Requests can only have 2, 3 or 4 parts, not {len(request)}"
            )
            return None, None

        try:
            packet = decode(data)
        except ValidationError as e:
            # The Router never crashes on input: bytes it cannot decode get a fast
            # MALFORMED_PACKET reply. reply_to is the id when the data parses as a JSON
            # object with a string id, and the reply carries the request-id frame the
            # message arrived with, even when reply_to is null.
            self.handle_packet_error(
                identity_binary,
                ErrorCode.MALFORMED_PACKET,
                f"Packet is not valid JSON for this protocol (a pre-v2 pickle peer sends non-JSON bytes): {e}",
                reply_to=_readable_reply_to(data),
                request_id=request_id,
            )
            return None, None
        if request_id is not None:
            # Only the latest request-id frame and request id of each Client is remembered.
            self.request_ids[identity_binary] = (request_id, packet.id)
        if packet.version != PROTOCOL_VERSION:
            # The Router enforces an exact version match, including at registration.
            self.handle_packet_error(
                identity_binary,
                ErrorCode.UNSUPPORTED_VERSION,
                f"Packet version {packet.version} is not supported, this Router speaks version {PROTOCOL_VERSION}.",
                reply_to=packet.id,
                details={"expected": PROTOCOL_VERSION, "received": packet.version},
            )
            return None, None
        if packet.source.encode("utf-8") != identity_binary:
            # Every element's Identity equals its name, and the Router rejects any Packet whose
            # source does not match the identity frame it arrived on.
            self.handle_packet_error(
                identity_binary,
                ErrorCode.MALFORMED_PACKET,
                f"Packet source '{packet.source}' does not match the identity frame it arrived on.",
                reply_to=packet.id,
            )
            return None, None
        logger.info("Received packet from %s: %s", identity_binary, packet)
        return identity_binary, packet

    def _send(self, destination: bytes, packet: Packet, request_id: bytes | None = None) -> None:
        # This should never happen, but mypy complains if the check is not done
        if self.socket is None:
            msg = "Socket is None, cannot send message."
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Sending packet to %s | Packet: %s", packet.destination, packet)
        frames = [destination, b"", encode(packet)]
        # A reply to a Client's latest request carries its request-id frame back, so the Client's
        # REQ socket accepts it; any other Packet goes without it, so the socket drops it.
        # An error reply to a message just received carries that message's request-id frame even
        # when its reply_to is null.
        frame = request_id
        if frame is None:
            remembered = self.request_ids.get(destination)
            if remembered is not None and packet.reply_to == remembered[1]:
                frame = remembered[0]
        if frame is not None:
            frames.insert(1, frame)
        self.socket.send_multipart(frames)
        logger.info("Packet sent to %s", packet.destination)

    def handle_packet_error(  # noqa: PLR0913
        self,
        destination: bytes,
        code: ErrorCode,
        message: str,
        reply_to: str | None = None,
        details: dict[str, Any] | None = None,
        request_id: bytes | None = None,
    ) -> None:
        logger.error(message)
        error_packet = Packet(
            intent=PacketIntent.ERROR,
            request=Request.ERROR,
            source=self.name,
            # The identity frame may not be valid UTF-8 (e.g. libzmq's auto identities); the raw
            # bytes stay the routing frame, the Packet only needs a readable text destination.
            destination=destination.decode("utf-8", errors="replace"),
            reply_to=reply_to,
            payload=ErrorPayload(code=code, message=message, origin=self.name, details=details or {}).model_dump(),
        )
        try:
            self._send(destination, error_packet, request_id=request_id)
        except zmq.error.ZMQError as e:
            # The peer the reply is addressed to is already gone; the Router never crashes on
            # its own replies.
            logger.warning("Could not send the error reply to %s: %s", destination, e)
