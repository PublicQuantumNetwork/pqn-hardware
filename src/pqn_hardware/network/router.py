import json
import logging
from typing import Any

import zmq
from pydantic import ValidationError

from pqn_hardware.network.packet import PROTOCOL_VERSION
from pqn_hardware.network.packet import ErrorCode
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import RegisterPayload
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode

logger = logging.getLogger(__name__)


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


# FIXME: handle not finding destination and source better
class Router:
    def __init__(self, name: str, host: str = "localhost", port: int = 5555) -> None:
        self.name = name
        self.host = host
        self.port = port

        # TODO: Verify that this address is valid
        self.address = f"tcp://{host}:{port}"

        # FIXME, breaking this into 3 different dictionaries is probably not the way to go.
        self.routers: dict[str, bytes] = {}  # Holds what other routers are in the network
        self.providers: dict[str, bytes] = {}
        self.clients: dict[str, bytes] = {}

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
                    match packet.intent:
                        case PacketIntent.REGISTRATION:
                            self.handle_registration(identity_binary, packet)
                        case _:
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

        finally:
            self.socket.close()

    def handle_registration(self, identity_binary: bytes, packet: Packet) -> None:
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
                self.providers[packet.source] = identity_binary
                logger.info("InstrumentProvider %s registered", identity_binary)
            case NetworkElementClass.CLIENT:
                self.clients[packet.source] = identity_binary
                logger.info("Client %s registered", identity_binary)

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
            if packet.intent == PacketIntent.ERROR:
                # An ERROR Packet is itself a reply and is never answered, so errors cannot
                # bounce forever.
                logger.warning("Received an ERROR Packet, which the Router never asked for: %s", packet)
            else:
                logger.info("Packet destination is self, dropping")

        elif packet.destination in self.providers or packet.destination in self.clients:
            logger.info("Packet destination is a provider called %s, routing message there", packet.destination)
            forward_packet = packet.model_copy(update={"hops": packet.hops + 1})
            dest = self.providers.get(packet.destination) or self.clients.get(packet.destination)
            if dest is None:
                self.handle_packet_error(
                    identity_binary,
                    ErrorCode.UNKNOWN_DESTINATION,
                    f"Destination {packet.destination} not found.",
                    reply_to=packet.id,
                )
                return
            self._send(dest, forward_packet)
            logger.info("Sent packet to %s", packet.destination)

        else:
            logger.info("Packet destination %s is not registered with this Router", packet.destination)
            self.handle_packet_error(
                identity_binary,
                ErrorCode.UNKNOWN_DESTINATION,
                f"Destination '{packet.destination}' is not registered with {self.name}. "
                f"Registered: {', '.join(self.providers)}",
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
        self._send(destination, error_packet, request_id=request_id)
