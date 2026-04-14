from __future__ import annotations

from concurrent.futures import as_completed

from ..contracts import AskTicket
from ..enums import IntentStatus, WorkflowStatus
from ..execution.intent_executor import IntentExecutionResult, IntentExecutor
from ..runtime.intent_worker_pool import LocalThreadWorkerPool
from ..state import WorkflowState


class IntentDispatcher:
    def __init__(self, executor: IntentExecutor, max_parallel_intents: int = 1) -> None:
        self.executor = executor
        self.max_parallel_intents = max(1, int(max_parallel_intents))

    def dispatch(self, workflow_state: WorkflowState) -> AskTicket | None:
        pool = LocalThreadWorkerPool(max_workers=self.max_parallel_intents)
        try:
            progress = True
            while progress:
                progress = False
                ready_states = []
                for intent_state in workflow_state.intents.values():
                    if intent_state.status in {IntentStatus.COMPLETED, IntentStatus.FAILED, IntentStatus.BLOCKED_BY_UPSTREAM, IntentStatus.WAIT_USER}:
                        continue
                    if any(workflow_state.intents[dep_id].status in {IntentStatus.FAILED, IntentStatus.BLOCKED_BY_UPSTREAM} for dep_id in intent_state.dependent_intent_ids if dep_id in workflow_state.intents):
                        intent_state.status = IntentStatus.BLOCKED_BY_UPSTREAM
                        progress = True
                        continue
                    if any(workflow_state.intents[dep_id].status != IntentStatus.COMPLETED for dep_id in intent_state.dependent_intent_ids if dep_id in workflow_state.intents):
                        continue
                    intent_state.status = IntentStatus.READY
                    ready_states.append(intent_state)
                if not ready_states:
                    continue
                futures = [pool.submit(self.executor.execute, intent_state, workflow_state) for intent_state in ready_states]
                for future in as_completed(futures):
                    result: IntentExecutionResult = future.result()
                    progress = True
                    if result.status == IntentStatus.WAIT_USER:
                        workflow_state.status = WorkflowStatus.WAIT_USER
                        ticket = result.ask_ticket
                        if ticket is not None:
                            workflow_state.ask_queue.tickets[ticket.ticket_id] = ticket
                            workflow_state.ask_queue.active_ticket_id = ticket.ticket_id
                        else:
                            ticket = self._active_ticket_for_intent(workflow_state, result.intent_id)
                        return ticket
        finally:
            pool.shutdown()
        completed = [item for item in workflow_state.intents.values() if item.status == IntentStatus.COMPLETED]
        failed = [item for item in workflow_state.intents.values() if item.status in {IntentStatus.FAILED, IntentStatus.BLOCKED_BY_UPSTREAM}]
        if completed and not failed:
            workflow_state.status = WorkflowStatus.COMPLETED
        elif completed and failed:
            workflow_state.status = WorkflowStatus.PARTIAL_SUCCESS
        else:
            workflow_state.status = WorkflowStatus.FAILED
        return None

    def _active_ticket_for_intent(self, workflow_state: WorkflowState, intent_id: str) -> AskTicket | None:
        for ticket_id in [workflow_state.ask_queue.active_ticket_id] + list(workflow_state.ask_queue.queued_ticket_ids):
            if not ticket_id:
                continue
            ticket = workflow_state.ask_queue.tickets.get(ticket_id)
            if ticket and ticket.owner_id == intent_id:
                return ticket
        for ticket in workflow_state.ask_queue.tickets.values():
            if ticket.owner_id == intent_id and ticket.status.name == "OPEN":
                workflow_state.ask_queue.active_ticket_id = ticket.ticket_id
                return ticket
        return None
