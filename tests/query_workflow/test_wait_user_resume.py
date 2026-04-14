from __future__ import annotations

import unittest

from stages.query_workflow.contracts import UserReply, WorkflowRequest
from stages.query_workflow.repositories.workflow_store import WorkflowStore
from stages.query_workflow.runtime.query_workflow_pipeline import QueryWorkflowPipeline


class WaitUserResumeTest(unittest.TestCase):
    def test_wait_user_then_resume_generates_next_ticket(self) -> None:
        store = WorkflowStore()
        pipeline = QueryWorkflowPipeline(store=store)
        request = WorkflowRequest(
            query="统计订单数量",
            database_scope=["missing_db_for_test"],
            model_name="missing-model",
        )
        first = pipeline.run(request)
        self.assertEqual(first.status.value, "WAIT_USER")
        self.assertIsNotNone(first.ask_ticket)

        resumed = pipeline.resume(
            first.workflow_id,
            UserReply(ticket_id=first.ask_ticket.ticket_id, reply="统计 order_data 里的订单总数"),
        )
        self.assertEqual(resumed.status.value, "WAIT_USER")
        self.assertIsNotNone(resumed.ask_ticket)
        self.assertTrue(str(resumed.ask_ticket.ticket_id).startswith("ask_"))


if __name__ == "__main__":
    unittest.main()
