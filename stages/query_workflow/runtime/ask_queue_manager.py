from __future__ import annotations

import hashlib

from ..contracts import AskTicket, UserReply
from ..enums import AskTicketStatus
from ..repositories.ask_queue_store import AskQueueStore
from ..state import AskQueueState, WorkflowState


class AskQueueManager:
    def __init__(self, store: AskQueueStore | None = None) -> None:
        self.store = store or AskQueueStore()

    def create_ticket(self, workflow_state: WorkflowState, ticket: AskTicket) -> AskTicket:
        ticket.fingerprint = self._fingerprint(ticket)
        for existing in workflow_state.ask_queue.tickets.values():
            if existing.fingerprint == ticket.fingerprint and existing.status == AskTicketStatus.OPEN:
                workflow_state.ask_queue.active_ticket_id = existing.ticket_id
                self.store.save(workflow_state.workflow_id, workflow_state.ask_queue)
                return existing
        workflow_state.ask_queue.tickets[ticket.ticket_id] = ticket
        if workflow_state.ask_queue.active_ticket_id is None:
            workflow_state.ask_queue.active_ticket_id = ticket.ticket_id
        else:
            workflow_state.ask_queue.queued_ticket_ids.append(ticket.ticket_id)
        self.store.save(workflow_state.workflow_id, workflow_state.ask_queue)
        return ticket

    def submit_reply(self, workflow_state: WorkflowState, reply: UserReply) -> AskTicket:
        ticket = workflow_state.ask_queue.tickets.get(reply.ticket_id)
        if ticket is None:
            raise ValueError("ticket_id not found")
        ticket.answer = reply.reply
        ticket.status = AskTicketStatus.ANSWERED
        if workflow_state.ask_queue.active_ticket_id == ticket.ticket_id:
            workflow_state.ask_queue.active_ticket_id = None
            self._promote_next(workflow_state.ask_queue)
        self.store.save(workflow_state.workflow_id, workflow_state.ask_queue)
        return ticket

    def sync(self, workflow_state: WorkflowState) -> None:
        self.store.save(workflow_state.workflow_id, workflow_state.ask_queue)

    def _promote_next(self, state: AskQueueState) -> None:
        while state.queued_ticket_ids:
            ticket_id = state.queued_ticket_ids.pop(0)
            ticket = state.tickets.get(ticket_id)
            if ticket and ticket.status == AskTicketStatus.OPEN:
                state.active_ticket_id = ticket_id
                return

    def _fingerprint(self, ticket: AskTicket) -> str:
        normalized = " ".join(ticket.question.split())
        raw = f"{ticket.scope}|{ticket.owner_id}|{ticket.question_id}|{normalized}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
