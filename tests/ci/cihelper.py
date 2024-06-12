from dataclasses import dataclass


@dataclass
class TestScript:
    path: str
    args: list[str] = None
    delay: float = 0.0
    displayOnPass: bool = False

    def __post_init__(self):
        if self.args is None:
            self.args = []

    def __str__(self):
        args_str = ":".join(self.args)
        return f"{self.path}:{args_str}"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.path, tuple(self.args)))

    @classmethod
    def from_existing(cls, existing, new_path):
        return cls(
            path=new_path, args=existing.args, delay=existing.delay, displayOnPass=existing.displayOnPass
        )
