---
name: sql-tester-bootstrap
description: Bootstrap and troubleshoot StarRocks SQL integration tests in `test/` by validating Python runtime compatibility, creating a compatible virtual environment, installing dependencies, and running minimal `run.py` checks to classify failures. Use when a user asks to run test scripts, debug `test/run.py`, or unblock SQL-tester execution failures.
---

# SQL Tester Bootstrap

Bring `test/run.py` to a runnable state quickly and identify whether failures come from:
- Python runtime incompatibility
- Missing Python dependencies
- SQL-tester configuration
- StarRocks cluster connectivity

## Workflow

1. Verify entrypoint and baseline environment.
2. Check Python compatibility with `nose`.
3. Prepare a dedicated Python 3.11 virtual environment.
4. Install `test/requirements.txt`.
5. Run a minimal smoke test case.
6. Classify the failure and report exact next action.

## Step 1: Baseline Checks

Run from repository root:

```bash
ls -la test
python3 --version
cd test && python3 run.py -l
```

Expected failure signatures:
- `ModuleNotFoundError: No module named 'nose'`: dependencies missing.
- `ModuleNotFoundError: No module named 'imp'` (from `nose`): Python too new for `nose` (e.g., Python 3.14).

## Step 2: Create Compatible Runtime (Python 3.11)

Use Python 3.11 for SQL-tester:

```bash
cd test
/opt/homebrew/bin/python3.11 -m venv .venv311
.venv311/bin/python -m pip install -r requirements.txt
```

If `.venv311` already exists, reuse it and reinstall only when dependencies are missing or stale.

## Step 3: Smoke Test With a Single Case

Avoid full-suite startup while debugging the runner. Use one small case:

```bash
cd test
.venv311/bin/python run.py -d sql/test_jit/R/test_jit_expr --case_filter test --skip_reruns -v -c 1 -t 30
```

Interpretation:
- If a case starts and ends with DB connection errors, the runner is healthy; cluster/config is the current blocker.
- If startup fails before case execution, continue debugging runtime/dependency issues first.

## Step 4: Failure Classification

- `Can't connect to MySQL server on '127.0.0.1'`:
  - `test/conf/sr.conf` points to unavailable FE (`host/port` wrong, cluster not started).
- `[Miss config] arrow_port`:
  - informational warning unless you explicitly run arrow-protocol tests.
- Long/large output with `-l`:
  - use `-d ... --case_filter ... -c 1` for targeted diagnosis instead of default broad collection.

## Step 5: Standard Commands To Hand Off

```bash
# 1) Bootstrap runtime
cd test
/opt/homebrew/bin/python3.11 -m venv .venv311
.venv311/bin/python -m pip install -r requirements.txt

# 2) Minimal validation
.venv311/bin/python run.py -d sql/test_jit/R/test_jit_expr --case_filter test --skip_reruns -v -c 1 -t 30

# 3) Full validation (after cluster is reachable)
.venv311/bin/python run.py -v
```
