# How is `tests/` used?

The `tests/` directory contains **unit tests** for the core engine and the new Router/Kernel/Governor modules. They are executed with `pytest`. The tests verify:

- **StateSync** – deterministic hashing, commit & delta generation, version‑gap recovery.
- **Router** – routing tables, direction enforcement, multi‑source support.
- **IPCTransport** – framing, emit/on_frame, connect/listen.
- **HTTPSSETransport** – `/update` and `/stream` endpoints.
- **SyncStateBridge** – integration of Router, Kernel, and Governor.
- **QoS** – priority queue eviction and conflation.

To run them:

```bash
# Install dev dependencies (if not already)
pip install -e '.[dev]'

# Run tests
pytest tests/
```

The tests are also run automatically in the GitHub Actions CI pipeline (`.github/workflows/ci.yml`), so any pull request must pass them.

If you haven't yet, you can add the `tests/` folder using the update script provided earlier. The script creates `tests/test_sync.py` with the tests above.

## alternative debugging of tests
python -X faulthandler -m pytest -o faulthandler_timeout=5 .