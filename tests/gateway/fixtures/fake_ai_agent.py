"""Mock AIAgent for desktop adapter tests."""
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class FakeAIAgent:
    """Simulates AIAgent.run_conversation for testing.

    Configure via constructor:
      - response: the text to return
      - delay: seconds to sleep before returning
      - raise_exc: exception to raise
      - approval_trigger: if set, calls the registered approval callback
    """
    response: str = "Hello from fake agent"
    delay: float = 0
    raise_exc: Optional[Exception] = None
    approval_trigger: Optional[dict] = None

    # Mimics AIAgent attributes the adapter reads
    session_prompt_tokens: int = 100
    session_completion_tokens: int = 50
    model: str = "test-model"
    _interrupted: bool = False

    def run_conversation(self, message: str = "", conversation_history=None,
                         task_id: str = "") -> dict:
        import time
        if self.delay:
            time.sleep(self.delay)
        if self.raise_exc:
            raise self.raise_exc
        return {"final_response": self.response}

    def interrupt(self, reason: str = "") -> None:
        self._interrupted = True
