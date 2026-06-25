import logging
import time

import zmq

from pqn_hardware.instrument import TimeTaggerInstrument

logger = logging.getLogger(__name__)


class ID900TimeTagger(TimeTaggerInstrument):
    def start(self) -> None:
        logger.info("Creating ID900 Time Tagger instance")
        context = zmq.Context()
        self._tagger = context.socket(zmq.REQ)
        ADDR = f"tcp://{self.hw_address}"
        self._tagger.connect(ADDR)

        for i in range(1, 5):
            SCPIcommand = f"INPU{i}:ENAB ON"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")

            SCPIcommand = f"INPU{i}:THRE 0.1"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")

        if not self._tagger:
            msg = "Failed to create time tagger. Verify hardware connection."
            logger.error(msg)
            raise RuntimeError(msg)

    def set_input_delay(self, channel: int, delay_ps: int) -> None:
        SCPIcommand = f"DELA{channel}:LINK INPU{channel}"
        self._tagger.send_string(SCPIcommand)
        ans = self._tagger.recv().decode("utf-8")
        SCPIcommand = f"DELA:VALU {delay_ps}"
        self._tagger.send_string(SCPIcommand)
        ans = self._tagger.recv().decode("utf-8")

    def count_singles(self, channels: list[int], integration_time_s: float = 1.0) -> list[int]:
        _duration_ms = float(integration_time_s * 1e3)
        counts = []
        for channel in channels:
            SCPIcommand = f"INPU{channel}:COUN:MODE CYCLE"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")
            SCPIcommand = f"INPU{channel}:COUN:INTE {_duration_ms}"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")
            time.sleep(integration_time_s)
            SCPIcommand = f"INPU{channel}:COUN?"
            self._tagger.send_string(SCPIcommand)
            answer = self._tagger.recv().decode("utf-8")
            counts.append(int(answer))
        return counts

    def measure_correlation(
        self,
        start_ch: int,
        stop_ch: int,
        integration_time_s: float = 1.0,
        coinc_window_ps: int = 1000,
        inwidth_ps: int = 1,  # noqa: ARG002 - unused; the Rabbit has no time-resolved histogram
        n_bins: int = int(1e5),  # noqa: ARG002 - unused; kept for interface compatibility
    ) -> int:
        count_time_ms = int(integration_time_s * 1e3)
        SCPIcommand = f"DELA{start_ch}:LINK?"
        self._tagger.send_string(SCPIcommand)
        ans = self._tagger.recv().decode("utf-8")
        if ans != "NONE":
            SCPIcommand = f"DELA5:LINK DELA{start_ch}"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")
        else:
            self.set_input_delay(start_ch, 0)
            SCPIcommand = f"DELA5:LINK DELA{start_ch}"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")
        SCPIcommand = f"DELA{stop_ch}:LINK?"
        self._tagger.send_string(SCPIcommand)
        ans = self._tagger.recv().decode("utf-8")
        if ans != "NONE":
            SCPIcommand = f"DELA6:LINK DELA{stop_ch}"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")
        else:
            self.set_input_delay(stop_ch, 0)
            SCPIcommand = f"DELA6:LINK DELA{stop_ch}"
            self._tagger.send_string(SCPIcommand)
            ans = self._tagger.recv().decode("utf-8")

        SCPIcommand = f"DELA{stop_ch}:VALU?"
        self._tagger.send_string(SCPIcommand)
        dela6_val = int(self._tagger.recv().decode("utf-8")[:-2]) + coinc_window_ps
        SCPIcommand = f"DELA6:VALU {dela6_val}"
        self._tagger.send_string(SCPIcommand)
        ans = self._tagger.recv().decode("utf-8")

        start_time = int(dela6_val - coinc_window_ps / 2)
        end_time = int(dela6_val + coinc_window_ps / 2)

        self._tagger.send_string(
            f"TSCO1:WIND:ENAB ON;BEGI:DELA {start_time};EDGE RISING;LINK DELA5:TSCO1:WIND:END:DELA {end_time};EDGE RISING;LINK DELA5;:TSCO1:FIR:LINK DELA6;:TSCO:SEC:LINK NONE;:TSCO1:OPIN ONLYFIR;OPOUT MUTE;:TSCO1:COUN:INTE {count_time_ms};MODE CYCLE"
        )
        ans = self._tagger.recv().decode("utf-8")

        self._tagger.send_string("TSCO1:COUN:RESET")
        ans = self._tagger.recv().decode("utf-8")

        time.sleep(integration_time_s)
        self._tagger.send_string("TSCO1:COUN?")
        answer = self._tagger.recv().decode("utf-8")
        return int(answer)
