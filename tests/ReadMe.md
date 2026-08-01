# How is `tests/` used?

The `tests/` directory contains **unit tests** for the core engine. They are executed with `pytest`. The tests verify:

- **Deterministic hashing** – ensures that two semantically identical objects (different order) produce the same hash.
- **Commit and delta generation** – checks that committing a dirty type produces correct deltas for a client with version 0.
- **Version‑gap recovery** – verifies that if a client version is outside the server's history, a full snapshot is sent.

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
