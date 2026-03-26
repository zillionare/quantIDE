"""Legacy local QMT compatibility helpers.

This module centralizes the opt-in gate for all remaining local QMT and
xtquant-based compatibility paths inside the pyqmt subject application.
"""

import os


LEGACY_LOCAL_QMT_ENV = "PYQMT_ENABLE_LEGACY_LOCAL_QMT"


def legacy_local_qmt_enabled() -> bool:
    """Return whether the legacy local QMT path is explicitly enabled."""
    return os.getenv(LEGACY_LOCAL_QMT_ENV, "").strip() == "1"


def ensure_legacy_local_qmt_enabled(
    feature_name: str,
    recommended_path: str = "qmt-gateway",
) -> None:
    """Require explicit opt-in before using a retired local QMT feature.

    Args:
        feature_name: Human-readable name of the retired feature.
        recommended_path: Preferred replacement path shown to the user.

    Raises:
        RuntimeError: If the compatibility flag is not enabled.
    """
    if legacy_local_qmt_enabled():
        return

    raise RuntimeError(
        f"{feature_name} 已退役为兼容路径，默认不可用。"
        f"请改用 {recommended_path}；若确需临时使用本地 QMT，请显式设置 "
        f"{LEGACY_LOCAL_QMT_ENV}=1。"
    )