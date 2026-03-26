import pytest

from pyqmt.service.qmt_broker import QMTBroker


def test_qmt_broker_is_removed_from_subject_app():
    with pytest.raises(RuntimeError, match="主体移除|qmt-gateway|QMTBroker"):
        QMTBroker("demo-account")