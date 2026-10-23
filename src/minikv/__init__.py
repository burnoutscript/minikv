"""minikv: banco chave-valor persistente no estilo Bitcask com servidor RESP."""

__version__ = "0.1.0"

from .client import Client
from .locks import StoreLockedError
from .resp import ProtocolError, RespError
from .store import CompactionResult, MiniKV, NotAnIntegerError, StoreClosedError

__all__ = [
    "Client",
    "CompactionResult",
    "MiniKV",
    "NotAnIntegerError",
    "ProtocolError",
    "RespError",
    "StoreClosedError",
    "StoreLockedError",
    "__version__",
]
