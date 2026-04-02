"""Shared init wizard step metadata.

Keep the wizard state machine, progress display, and page navigation aligned
through a single source of truth.
"""

from __future__ import annotations


WIZARD_STEP_DEFINITIONS: tuple[tuple[int, str], ...] = (
    (1, "欢迎"),
    (2, "运行环境"),
    (3, "管理员密码"),
    (4, "行情与交易网关"),
    (5, "数据源设置及下载"),
    (6, "完成"),
)

WIZARD_TOTAL_STEPS = len(WIZARD_STEP_DEFINITIONS)
WIZARD_FINAL_STEP = WIZARD_STEP_DEFINITIONS[-1][0]


def build_wizard_steps(current_step: int) -> list[dict[str, int | str | bool]]:
    """Build the shared step progress structure for the init wizard."""
    steps: list[dict[str, int | str | bool]] = []
    for step_id, name in WIZARD_STEP_DEFINITIONS:
        completed = current_step >= step_id if step_id == WIZARD_FINAL_STEP else current_step > step_id
        steps.append({"id": step_id, "name": name, "completed": completed})
    return steps


__all__ = [
    "WIZARD_FINAL_STEP",
    "WIZARD_STEP_DEFINITIONS",
    "WIZARD_TOTAL_STEPS",
    "build_wizard_steps",
]
