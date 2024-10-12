from dataclasses import dataclass
from enum import Enum


class PodType(Enum):
    SFM_WORKER = "SFM_WORKER"
    PSF_PLOTTER = "PSF_PLOTTER"
    NIGHTLYROLLUP_WORKER = "NIGHTLYROLLUP_WORKER"
    STEP2A_WORKER = "STEP2A_WORKER"
    MOSAIC_WORKER = "MOSAIC_WORKER"

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
    detectorNumber: int
    depth: int
    queueName: str

    def __init__(self, instrument: str, podType: PodType, detectorNumber: int, depth: int):
        self.instrument = instrument
        self.podType = podType
        self.detectorNumber = detectorNumber
        self.depth = depth
        self.queueName = f"{self.instrument}-{self.podType.value}-{self.detectorNumber:03d}-{self.depth:03d}"

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
