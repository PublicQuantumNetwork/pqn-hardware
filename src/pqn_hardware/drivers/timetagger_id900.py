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
        addr = f"tcp://{self.hw_address}"
        self._tagger.connect(addr)

        for i in range(1, 5):
            command = f"INPU{i}:ENAB ON"
            self._tagger.send_string(command)
            self._tagger.recv()

            command = f"INPU{i}:THRE 0.1"
            self._tagger.send_string(command)
            self._tagger.recv()

        if not self._tagger:
            msg = "Failed to create time tagger. Verify hardware connection."
            logger.error(msg)
            raise RuntimeError(msg)

    def set_input_delay(self, channel: int, delay_ps: int) -> None:
        command = f"DELA{channel}:LINK INPU{channel}"
        self._tagger.send_string(command)
        self._tagger.recv()
        command = f"DELA:VALU {delay_ps}"
        self._tagger.send_string(command)
        self._tagger.recv()

    def count_singles(self, channels: list[int], integration_time_s: float = 1.0) -> list[int]:
        _duration_ms = float(integration_time_s * 1e3)
        counts = []
        for channel in channels:
            command = f"INPU{channel}:COUN:MODE CYCLE"
            self._tagger.send_string(command)
            self._tagger.recv()
            command = f"INPU{channel}:COUN:INTE {_duration_ms}"
            self._tagger.send_string(command)
            self._tagger.recv()
            time.sleep(integration_time_s)
            command = f"INPU{channel}:COUN?"
            self._tagger.send_string(command)
            answer = self._tagger.recv().decode("utf-8")
            counts.append(int(answer))
        return counts

    def measure_correlation(
        self,
        start_ch: int,
        stop_ch: int,
        integration_time_s: float = 1.0,
        binwidth_ps: int = 1000,
    ) -> int:
        """Count coincidences between ``start_ch`` and ``stop_ch`` over ``integration_time_s``.

        The ID900 builds no histogram, so ``binwidth_ps`` is used as the coincidence window: a
        stop click counts if it lands within ``binwidth_ps / 2`` of a start click (after the
        configured input delays). This matches how pqn-node uses ``binwidth_ps``: its
        ``coincidence_window_ps`` API parameter is passed straight through as ``binwidth_ps``.

        Unlike the Swabian driver, which reports the busiest bin of a histogram wherever the
        peak falls, the window here sits at a fixed position set by the input delays.
        """
        coinc_window_ps = binwidth_ps
        count_time_ms = int(integration_time_s * 1e3)
        command = f"DELA{start_ch}:LINK?"
        self._tagger.send_string(command)
        ans = self._tagger.recv().decode("utf-8")
        if ans != "NONE":
            command = f"DELA5:LINK DELA{start_ch}"
            self._tagger.send_string(command)
            self._tagger.recv()
        else:
            self.set_input_delay(start_ch, 0)
            command = f"DELA5:LINK DELA{start_ch}"
            self._tagger.send_string(command)
            self._tagger.recv()
        command = f"DELA{stop_ch}:LINK?"
        self._tagger.send_string(command)
        ans = self._tagger.recv().decode("utf-8")
        if ans != "NONE":
            command = f"DELA6:LINK DELA{stop_ch}"
            self._tagger.send_string(command)
            self._tagger.recv()
        else:
            self.set_input_delay(stop_ch, 0)
            command = f"DELA6:LINK DELA{stop_ch}"
            self._tagger.send_string(command)
            self._tagger.recv()

        command = f"DELA{stop_ch}:VALU?"
        self._tagger.send_string(command)
        dela6_val = int(self._tagger.recv().decode("utf-8")[:-2]) + coinc_window_ps
        command = f"DELA6:VALU {dela6_val}"
        self._tagger.send_string(command)
        self._tagger.recv()

        start_time = int(dela6_val - coinc_window_ps / 2)
        end_time = int(dela6_val + coinc_window_ps / 2)

        self._tagger.send_string(
            f"TSCO1:WIND:ENAB ON;BEGI:DELA {start_time};EDGE RISING;LINK DELA5:TSCO1:WIND:END:DELA {end_time};EDGE RISING;LINK DELA5;:TSCO1:FIR:LINK DELA6;:TSCO:SEC:LINK NONE;:TSCO1:OPIN ONLYFIR;OPOUT MUTE;:TSCO1:COUN:INTE {count_time_ms};MODE CYCLE"
        )
        self._tagger.recv()

        self._tagger.send_string("TSCO1:COUN:RESET")
        self._tagger.recv()

        time.sleep(integration_time_s)
        self._tagger.send_string("TSCO1:COUN?")
        answer = self._tagger.recv().decode("utf-8")
        return int(answer)
