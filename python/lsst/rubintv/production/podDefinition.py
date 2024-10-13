from dataclasses import dataclass
from enum import Enum


class PodType(Enum):
    SFM_WORKER = "SFM_WORKER"
    PSF_PLOTTER = "PSF_PLOTTER"
    NIGHTLYROLLUP_WORKER = "NIGHTLYROLLUP_WORKER"
    STEP2A_WORKER = "STEP2A_WORKER"
    MOSAIC_WORKER = "MOSAIC_WORKER"

    HEAD_NODE = "HEAD_NODE"

    @classmethod
    def validate_values(cls):
        for item in cls:
            if "-" in item.value:
                raise ValueError(f"Invalid PodType: value with dash: {item.value}")


# trigger this check import, as this is covered by tests: ensures that nobody
# ever adds a type with a dash in it
PodType.validate_values()


@dataclass(kw_only=True)
class PodDetails:
    instrument: str
    podType: PodType
    detectorNumber: int | None
    depth: int | None
    queueName: str

    def _getHeadNodeName(self, instrument):
        return f"headNode-{instrument}"

    def __init__(self, instrument: str, podType: PodType, detectorNumber: int | None, depth: int | None):
        self.instrument = instrument
        self.podType = podType
        self.detectorNumber = detectorNumber
        self.depth = depth
        self.queueName = self._getQueueName()
        self.validate()

    def _getQueueName(self):
        if self.podType == PodType.HEAD_NODE:
            return self._getHeadNodeName(self.instrument)
        return f"{self.instrument}-{self.podType.value}-{self.detectorNumber:03d}-{self.depth:03d}"

    def validate(self):
        if self.podType == PodType.HEAD_NODE:
            if self.detectorNumber is not None or self.depth is not None:
                raise ValueError(f"Expected None for both detectorNumber and depth for {self.podType}")

    @classmethod
    def fromQueueName(cls, queueName: str):
        parts = queueName.split("-")

        if len(parts) != 4:
            raise ValueError(f"Expected 4 parts in the input string, but got {len(parts)}: {queueName}")

        instrument = parts[0]
        podType = PodType(parts[1])
        detectorNumber = int(parts[2])
        depth = int(parts[3])

        return cls(instrument=instrument, podType=podType, detectorNumber=detectorNumber, depth=depth)
