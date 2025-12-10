from typing import Optional


class SemverError(Exception):
    def __init__(self, msg: Optional[str] = None) -> None:
        self.msg = msg
        if msg is not None:
            super().__init__(msg)
        else:
            super().__init__()


class VersionsNotCompatibleError(SemverError):
    pass


class FusionBinaryNotAvailable(Exception):
    def __init__(self, message="Fusion binary not found on system, please install Fusion first"):
        self.message = message
        super().__init__(self.message)


class GitOperationError(Exception):
    """Custom exception for git operation failures"""

    pass
