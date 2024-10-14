from dataclasses import dataclass
from enum import Enum

__all__ = ["PodType", "PodFlavor", "PodDetails", "getQueueName"]

DELIMITER = "-"


class PodType(Enum):
    PER_DETECTOR = "PER_DETECTOR"  # has depth and detectorNumber
    PER_INSTRUMENT = "PER_INSTRUMENT"  # has depth, but no detectorNumber
    PER_INSTRUMENT_SINGLETON = "PER_INSTRUMENT_SINGLETON"  # has neither depth nor detectorNumber


class PodFlavor(Enum):
    # all items much contain WORKER if they're not the head node
    # all items must also provide their type via an entry in podFlavorToPodType
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
                raise ValueError(f"Invalid PodFlavor: value with dash: {item.value}")


# trigger this check import, as this is covered by tests: ensures that nobody
# ever adds a type with a dash in it
PodFlavor.validate_values()


def podFlavorToPodType(podFlavor: PodFlavor) -> PodType:
    mapping = {
        PodFlavor.HEAD_NODE: PodType.PER_INSTRUMENT_SINGLETON,
        PodFlavor.SFM_WORKER: PodType.PER_DETECTOR,
        PodFlavor.PSF_PLOTTER: PodType.PER_INSTRUMENT,
        PodFlavor.NIGHTLYROLLUP_WORKER: PodType.PER_INSTRUMENT,
        PodFlavor.STEP2A_WORKER: PodType.PER_INSTRUMENT,
        PodFlavor.MOSAIC_WORKER: PodType.PER_INSTRUMENT,
    }
    return mapping[podFlavor]


def getQueueName(
    podFlavor: PodFlavor, instrument: str, detectorNumber: int | str | None, depth: int | str | None
) -> str:
    podType = podFlavorToPodType(podFlavor)
    queueName = f"{podFlavor.value}{DELIMITER}{instrument}"

    if podType == PodType.PER_INSTRUMENT_SINGLETON:
        return queueName

    queueName += f"{DELIMITER}{depth:03d}" if isinstance(depth, int) else f"{DELIMITER}{detectorNumber}"
    if podType == PodType.PER_INSTRUMENT:
        return queueName

    queueName += (
        f"{DELIMITER}{detectorNumber:03d}"
        if isinstance(detectorNumber, int)
        else f"{DELIMITER}{detectorNumber}"
    )
    return queueName


@dataclass(kw_only=True)
class PodDetails:
    instrument: str
    podFlavor: PodFlavor
    podType: PodType
    detectorNumber: int | None
    depth: int | None
    queueName: str

    def __init__(self, instrument: str, podFlavor: PodFlavor, detectorNumber: int | None, depth: int | None):
        # set attributes first so they don't have to passed around
        self.instrument: str = instrument
        self.podFlavor: PodFlavor = podFlavor
        self.detectorNumber: int | None = detectorNumber
        self.depth: int | None = depth
        self.podType: PodType = podFlavorToPodType(podFlavor)

        # then call validate to check this is legal
        self.validate()

        # then set the queueName from the properties, now that they are legal
        self.queueName = getQueueName(
            podFlavor=self.podFlavor,
            instrument=self.instrument,
            detectorNumber=self.detectorNumber,
            depth=self.depth,
        )

    def __lt__(self, other):
        if not isinstance(other, PodDetails):
            return NotImplementedError(f"Cannot compare PodDetails with {type(other)}")
        return self.queueName < other.queueName

    def validate(self):
        if self.podType == PodType.PER_INSTRUMENT_SINGLETON:
            if self.detectorNumber is not None or self.depth is not None:
                raise ValueError(f"Expected None for both detectorNumber and depth for {self.podFlavor}")

        if self.podType == PodType.PER_INSTRUMENT:
            if self.detectorNumber is not None:
                raise ValueError(f"Expected None for detectorNumber per-instrument {self.podFlavor}")
            if self.depth is None:
                raise ValueError(f"Depth is required for per-instrument non-singleton pods {self.podFlavor}")

        if self.podType == PodType.PER_DETECTOR:
            if self.detectorNumber is None or self.depth is None:
                raise ValueError(f"Both detectorNumber and depth required for per-detector {self.podFlavor}")

    @classmethod
    def fromQueueName(cls, queueName: str):
        parts = queueName.split(DELIMITER)

        if len(parts) < 2 or len(parts) > 4:
            raise ValueError(f"Expected 2 to 4 parts in the input string, but got {len(parts)}: {queueName}")

        podFlavor = PodFlavor(parts[0])
        instrument = parts[1]
        depth = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        detectorNumber = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

        return cls(instrument=instrument, podFlavor=podFlavor, detectorNumber=detectorNumber, depth=depth)
