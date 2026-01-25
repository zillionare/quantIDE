
import asyncio
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import SimulationBroker
from pyqmt.core.enums import BrokerKind

def test_registry():
    reg = BrokerRegistry()
    broker = SimulationBroker("test_sim", principal=10000)
    reg.register(broker)
    
    assert reg.get("simulation", "test_sim") == broker
    assert reg.get(BrokerKind.SIMULATION, "test_sim") == broker
    assert reg.get_default() == ("simulation", "test_sim")
    
    # Test properties
    assert broker.cash == 10000
    assert broker.principal == 10000
    assert broker.positions == []
    
    print("Registry and Broker properties test passed!")

if __name__ == "__main__":
    test_registry()
