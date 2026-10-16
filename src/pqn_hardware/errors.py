from pqn_hardware.network.packet import ErrorPayload


class DeviceNotStartedError(Exception):
    def __init__(self, message: str = "Device not started") -> None:
        self.message = message
        super().__init__(self.message)


class DriverNotFoundError(Exception):
    def __init__(self, message: str = "Device driver configuration not found") -> None:
        self.message = message
        super().__init__(self.message)


class DriverFunctionNotImplementedError(Exception):
    def __init__(self, message: str = "One or more driver functions were not implemented") -> None:
        self.message = message
        super().__init__(self.message)


class DriverFunctionUnknownError(Exception):
    def __init__(self, message: str = "Device driver function unknown") -> None:
        self.message = message
        super().__init__(self.message)


class LogDecoratorOutsideOfClassError(Exception):
    def __init__(self, message: str = "Log decorator used outside of a class") -> None:
        self.message = message
        super().__init__(self.message)


class PacketError(Exception):
    """An ERROR reply Packet, carrying its ErrorPayload.

    The Payload's code, origin and details are attributes, so callers branch on the error code,
    never on the message text.
    """

    def __init__(self, message: str = "Packet error", payload: ErrorPayload | None = None) -> None:
        self.message = message
        self.payload = payload
        self.code = payload.code if payload is not None else None
        self.origin = payload.origin if payload is not None else None
        self.details = payload.details if payload is not None else {}
        super().__init__(self.message)


class InvalidInstrumentsConfigurationError(Exception):
    def __init__(self, message: str = "Invalid instruments configuration") -> None:
        self.message = message
        super().__init__(self.message)


class InvalidNetworkConfigurationError(Exception):
    def __init__(self, message: str = "Invalid network configuration") -> None:
        self.message = message
        super().__init__(self.message)


class CouldNotConnectToNetworkElementError(Exception):
    def __init__(self, message: str = "Could not connect to network element") -> None:
        self.message = message
        super().__init__(self.message)
