from __future__ import annotations

from app.task_registry import available_task_types, get_task, register_task


def test_registry_exposes_built_in_task_types():
    task_types = available_task_types()

    assert "dummy" in task_types
    assert "image_resize" in task_types
    assert "word_count" in task_types
    assert "fibonacci" in task_types


def test_registry_can_register_new_task_type():
    @register_task("registry_test_task")
    def _handler(value: int, **kwargs):
        return value + 1

    handler = get_task("registry_test_task")
    assert handler is not None
    assert handler(4) == 5
