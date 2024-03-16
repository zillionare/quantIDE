import uuid
from typing import Any, Callable, Optional, Tuple


class Job:
    """自带执行体的任务。
    
    本类及其子类必须是可串行化(serializable)的。
    """
    def __init__(self, target: Callable, args: Tuple[Any], name: Optional[str] = None):
        self.target = target
        self.args = args
        self.name = name or uuid.uuid4().hex[-6:]

    def run(self, results_queue):
        result = self.target(*self.args)
        if results_queue is not None:
            results_queue.put_nowait(result)

class StopOnSightJob(Job):
    """执行体在见到此任务后，将执行退出进程操作"""
    def __init__(self):
        pass
