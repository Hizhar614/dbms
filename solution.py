from Planner import *
from RelationalOp import *

# =========================================================================
# HELPER FUNCTIONS
# =========================================================================
def _get_term_fields(term):
    lhs = term.lhs.exp_value
    rhs = term.rhs.exp_value
    lhs_field = lhs if isinstance(lhs, str) else None
    rhs_field = rhs if isinstance(rhs, str) else None
    return lhs_field, rhs_field, None, None


def _make_predicate(terms):
    p = Predicate()
    p.terms = list(terms)
    return p

# =========================================================================
# 1. OPT MODE: Query Optimization (Upgraded for FULL mode)
# =========================================================================
class BetterQueryPlanner:
    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data, prebuilt_plans=None, indexes=None):
        prebuilt_plans = prebuilt_plans or {}
        indexes = indexes or {}  # Brought in from full mode!
        predicate = query_data.get('predicate')
        tables = query_data['tables']

        def term_is_covered(term, schema):
            lhs_f, rhs_f, _, _ = _get_term_fields(term)
            if lhs_f and lhs_f not in schema.field_info: return False
            if rhs_f and rhs_f not in schema.field_info: return False
            return True

        # ---------------------------------------------------------
        # Step 1: Create table plans & Pushdown Selections
        # ---------------------------------------------------------
        plans = []
        for table in tables:
            if table in prebuilt_plans:
                tp = prebuilt_plans[table]
            else:
                tp = TablePlan(tx, table, self.mm)
                if predicate:
                    local_terms = [t for t in predicate.terms if term_is_covered(t, tp.plan_schema())]
                    if local_terms:
                        tp = SelectPlan(tp, _make_predicate(local_terms))
            # Keep track of the raw table name so we can look up its indexes later
            plans.append({'name': table, 'plan': tp})

        # ---------------------------------------------------------
        # Step 2: Connectivity-Aware Greedy Join
        # ---------------------------------------------------------
        plans.sort(key=lambda p: p['plan'].recordsOutput() or 1)
        
        first_node = plans.pop(0)
        plan = first_node['plan']

        while plans:
            current_schema = plan.plan_schema()
            best_idx = 0  
            
            # Find a connected table
            for i, p in enumerate(plans):
                p_schema = p['plan'].plan_schema()
                connects = False
                if predicate:
                    for term in predicate.terms:
                        lhs_f, rhs_f, _, _ = _get_term_fields(term)
                        if lhs_f and rhs_f:
                            if (lhs_f in current_schema.field_info and rhs_f in p_schema.field_info) or \
                               (rhs_f in current_schema.field_info and lhs_f in p_schema.field_info):
                                connects = True
                                break
                if connects:
                    best_idx = i
                    break  
            
            next_node = plans.pop(best_idx)
            next_table = next_node['name']
            next_plan = next_node['plan']
            
            # 🔥 THE FULL MODE UPGRADE: Use an Index Join if possible!
            joined_with_index = False
            if predicate and next_table in indexes:
                t_indexes = indexes[next_table]
                for term in predicate.terms:
                    lhs_f, rhs_f, _, _ = _get_term_fields(term)
                    if lhs_f and rhs_f:
                        if lhs_f in current_schema.field_info and rhs_f in t_indexes:
                            plan = IndexJoinPlan(plan, tx, next_table, self.mm, t_indexes[rhs_f], lhs_f)
                            joined_with_index = True
                            break
                        elif rhs_f in current_schema.field_info and lhs_f in t_indexes:
                            plan = IndexJoinPlan(plan, tx, next_table, self.mm, t_indexes[lhs_f], rhs_f)
                            joined_with_index = True
                            break
            
            # Fallback to standard nested loop if no index exists
            if not joined_with_index:
                plan = ProductPlan(plan, next_plan)

            # Apply safe filters immediately after joining
            current_schema = plan.plan_schema()
            if predicate:
                join_terms = [t for t in predicate.terms if term_is_covered(t, current_schema)]
                if join_terms:
                    plan = SelectPlan(plan, _make_predicate(join_terms))

        return ProjectPlan(plan, *query_data['fields'])

# ... (Keep your IndexScan, IndexPlan, IndexJoinPlan, etc. right here) ...

    # =========================================================================
# =========================================================================
# =========================================================================
# 2. INDEX MODE: Index Structures & Execution
# =========================================================================

class BTreeIndex:
    def __init__(self):
        self.index = {}

    def insert(self, key, rid):
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(rid)

    def search(self, key):
        return self.index.get(key, [])

class CompositeIndex:
    def __init__(self):
        self.index = {}

    def insert(self, key_tuple, rid):
        if key_tuple not in self.index:
            self.index[key_tuple] = []
        self.index[key_tuple].append(rid)

    def search(self, key_tuple):
        return self.index.get(key_tuple, [])

class IndexScan:
    """Uses RIDs from an index to jump instantly to the right blocks."""
    def __init__(self, ts, rids):
        self.ts = ts
        self.rids = rids
        self.pos = -1

    def beforeFirst(self): self.pos = -1

    def nextRecord(self):
        self.pos += 1
        if self.pos < len(self.rids):
            self.ts.moveToRecordID(self.rids[self.pos])
            return True
        return False

    def getInt(self, f): return self.ts.getInt(f)
    def getString(self, f): return self.ts.getString(f)
    def getVal(self, f): return self.ts.getVal(f)
    def hasField(self, f): return self.ts.hasField(f)
    def closeRecordPage(self): self.ts.closeRecordPage()

class DummySchema:
    """Helper to mock a merged schema without import loops."""
    def __init__(self, fields):
        self.field_info = fields

class IndexPlan:
    """Wraps IndexScan so the Query Planner can treat it like a table."""
    def __init__(self, tx, table, mm, rids):
        self.tx = tx
        self.table = table
        self.mm = mm
        self.rids = rids
        self.layout = mm.getLayout(tx, table)

    def open(self):
        ts = TableScan(self.tx, self.table, self.layout)
        return IndexScan(ts, self.rids)

    def blocksAccessed(self): return len(self.rids)
    def recordsOutput(self): return len(self.rids)
    def distinctValues(self, f): return 1
    def plan_schema(self): return self.layout.schema


# -------------------------------------------------------------------------
# NEW: Index Nested Loop Join (Beats the Cartesian Explosion)
# -------------------------------------------------------------------------
class IndexJoinScan:
    def __init__(self, scan1, index2, join_field1, tx, table2, layout2):
        self.scan1 = scan1
        self.index2 = index2
        self.join_field1 = join_field1
        self.ts2 = TableScan(tx, table2, layout2)
        self.rids2 = []
        self.pos = -1
        self.scan1.beforeFirst()
        self._fetch_next_outer()

    def _fetch_next_outer(self):
        while self.scan1.nextRecord():
            val = self.scan1.getVal(self.join_field1)
            self.rids2 = self.index2.search(val)
            if self.rids2:
                self.pos = -1
                return True
        return False

    def beforeFirst(self):
        self.scan1.beforeFirst()
        self._fetch_next_outer()

    def nextRecord(self):
        while True:
            self.pos += 1
            if self.pos < len(self.rids2):
                self.ts2.moveToRecordID(self.rids2[self.pos])
                return True
            if not self._fetch_next_outer():
                return False

    def getInt(self, f): return self.scan1.getInt(f) if self.scan1.hasField(f) else self.ts2.getInt(f)
    def getString(self, f): return self.scan1.getString(f) if self.scan1.hasField(f) else self.ts2.getString(f)
    def getVal(self, f): return self.scan1.getVal(f) if self.scan1.hasField(f) else self.ts2.getVal(f)
    def hasField(self, f): return self.scan1.hasField(f) or self.ts2.hasField(f)
    def closeRecordPage(self):
        self.scan1.closeRecordPage()
        self.ts2.closeRecordPage()

class IndexJoinPlan:
    def __init__(self, plan1, tx, table2, mm, index2, join_field1):
        self.plan1 = plan1
        self.tx = tx
        self.table2 = table2
        self.mm = mm
        self.index2 = index2
        self.join_field1 = join_field1
        self.layout2 = mm.getLayout(tx, table2)
        
        merged = {**self.plan1.plan_schema().field_info, **self.layout2.schema.field_info}
        self.schema = DummySchema(merged)

    def open(self):
        scan1 = self.plan1.open()
        return IndexJoinScan(scan1, self.index2, self.join_field1, self.tx, self.table2, self.layout2)

    def blocksAccessed(self): return 1
    def recordsOutput(self): return 1
    def distinctValues(self, f): return 1
    def plan_schema(self): return self.schema

# -------------------------------------------------------------------------
# Index Query Planner
# -------------------------------------------------------------------------
class IndexQueryPlanner:
    def __init__(self, mm, indexes, better_planner=None):
        self.mm = mm
        self.indexes = indexes
        self.better_planner = better_planner

    def createPlan(self, tx, query_data):
        tables = query_data['tables']
        predicate = query_data.get('predicate')
        terms = list(predicate.terms) if predicate else []

        prebuilt_plans = {}
        equality_filters = {}
        
        for term in terms:
            lhs_f, rhs_f, lhs_c, rhs_c = _get_term_fields(term)
            if lhs_f and rhs_c is not None:
                equality_filters[lhs_f] = rhs_c.const_value
            elif rhs_f and lhs_c is not None:
                equality_filters[rhs_f] = lhs_c.const_value

        for t in tables:
            table_indexes = self.indexes.get(t, {})
            used_index = False

            for idx_name, idx_obj in table_indexes.items():
                if isinstance(idx_name, tuple):
                    if all(f in equality_filters for f in idx_name):
                        search_key = tuple(equality_filters[f] for f in idx_name)
                        rids = idx_obj.search(search_key)
                        prebuilt_plans[t] = IndexPlan(tx, t, self.mm, rids)
                        used_index = True
                        break

            if not used_index:
                for idx_name, idx_obj in table_indexes.items():
                    if isinstance(idx_name, str) and idx_name in equality_filters:
                        rids = idx_obj.search(equality_filters[idx_name])
                        prebuilt_plans[t] = IndexPlan(tx, t, self.mm, rids)
                        used_index = True
                        break

        # 🔥 FULL MODE: Hand over to BetterQueryPlanner, but pass the indexes!
        if self.better_planner:
            return self.better_planner.createPlan(tx, query_data, prebuilt_plans=prebuilt_plans, indexes=self.indexes)
        
        # INDEX MODE: Maintain original table order, but use IndexJoins
        else:
            plan = prebuilt_plans.get(tables[0]) or TablePlan(tx, tables[0], self.mm)
            
            for t in tables[1:]:
                joined_with_index = False
                if predicate:
                    for term in terms:
                        lhs_f, rhs_f, _, _ = _get_term_fields(term)
                        if lhs_f and rhs_f:
                            current_fields = plan.plan_schema().field_info
                            t_indexes = self.indexes.get(t, {})
                            
                            if lhs_f in current_fields and rhs_f in t_indexes:
                                plan = IndexJoinPlan(plan, tx, t, self.mm, t_indexes[rhs_f], lhs_f)
                                joined_with_index = True
                                break
                            elif rhs_f in current_fields and lhs_f in t_indexes:
                                plan = IndexJoinPlan(plan, tx, t, self.mm, t_indexes[lhs_f], rhs_f)
                                joined_with_index = True
                                break
                                
                if not joined_with_index:
                    next_plan = prebuilt_plans.get(t) or TablePlan(tx, t, self.mm)
                    plan = ProductPlan(plan, next_plan)
            
            if predicate:
                plan = SelectPlan(plan, predicate)
            
            return ProjectPlan(plan, *query_data['fields'])
# =========================================================================
# 3. CREATE INDEXES
# =========================================================================
def create_indexes(db, tx, index_defs, composite_index_defs):
    indexes = {}

    for table in set(index_defs.keys()) | set(composite_index_defs.keys()):
        indexes[table] = {}
        layout = db.mm.getLayout(tx, table)
        ts = TableScan(tx, table, layout)
        
        if table in index_defs:
            for idx_def in index_defs[table]:
                field_name = idx_def[0]
                indexes[table][field_name] = BTreeIndex()
                
        if table in composite_index_defs:
            for comp_def in composite_index_defs[table]:
                field_tuple = comp_def[0]
                indexes[table][field_tuple] = CompositeIndex()

        ts.beforeFirst()
        while ts.nextRecord():
            rid = ts.currentRecordID()
            
            if table in index_defs:
                for idx_def in index_defs[table]:
                    field_name = idx_def[0]
                    val = ts.getVal(field_name)
                    indexes[table][field_name].insert(val, rid)
                    
            if table in composite_index_defs:
                for comp_def in composite_index_defs[table]:
                    field_tuple = comp_def[0]
                    val_tuple = tuple(ts.getVal(f) for f in field_tuple)
                    indexes[table][field_tuple].insert(val_tuple, rid)
                    
        ts.closeRecordPage()

    return indexes
