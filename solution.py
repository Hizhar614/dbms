from Planner import *
from RelationalOp import *
class BetterQueryPlanner:
    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data):
        plans = []

        # Step 1: create table plans
        for table in query_data['tables']:
            tp = TablePlan(tx, table, self.mm)

            # 🔥 Selection pushdown
            if query_data['predicate']:
                tp = SelectPlan(tp, query_data['predicate'])

            plans.append(tp)

        # 🔥 Join order optimization (simple heuristic)
        # smallest table first
        plans.sort(key=lambda p: p.recordsOutput())

        # Step 2: build join (product)
        plan = plans[0]
        for p in plans[1:]:
            plan = ProductPlan(plan, p)

        # Step 3: projection
        return ProjectPlan(plan, *query_data['fields'])
