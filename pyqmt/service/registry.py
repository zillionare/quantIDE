from typing import Dict, List, Tuple

from pyqmt.core.enums import BrokerKind
from pyqmt.core.singleton import singleton
from pyqmt.service.base_broker import Broker


@singleton
class BrokerRegistry:
    def __init__(self):
        self._brokers: Dict[str, Broker] = {}
        self._default: Tuple[str, str] | None = None

    def register(self, broker: Broker):
        kind = broker.kind
        if isinstance(kind, BrokerKind):
            kind = kind.value
            
        key = f"{kind}:{broker.portfolio_id}"
        self._brokers[key] = broker
        if self._default is None:
            self._default = (kind, broker.portfolio_id)

    def unregister(self, kind: BrokerKind | str, portfolio_id: str):
        if isinstance(kind, BrokerKind):
            kind = kind.value
        key = f"{kind}:{portfolio_id}"
        if key in self._brokers:
            del self._brokers[key]
            
        if self._default == (kind, portfolio_id):
            if self._brokers:
                first_key = next(iter(self._brokers))
                k, p = first_key.split(":")
                self._default = (k, p)
            else:
                self._default = None

    def get(self, kind: BrokerKind | str, portfolio_id: str) -> Broker | None:
        if isinstance(kind, BrokerKind):
            kind = kind.value
        key = f"{kind}:{portfolio_id}"
        return self._brokers.get(key)
    
    def list(self) -> List[Dict]:
        return [{"kind": k.split(":")[0], "id": k.split(":")[1]} for k in self._brokers.keys()]

    def get_default(self) -> Tuple[str, str] | None:
        return self._default
