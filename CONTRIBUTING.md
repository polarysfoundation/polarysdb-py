# Contributing

Thanks for your interest in contributing to PolarysDB (Python edition).

## Development setup

- Python: 3.9+ (tests currently run with the system `python3`)

Install (editable):

```bash
python3 -m pip install -e .
```

## Running tests

This repo supports `unittest` out of the box:

```bash
python3 -m unittest discover -s tests -p 'test*.py' -q
```

If you prefer `pytest`, install dev deps from `pyproject.toml` and run:

```bash
python3 -m pytest -q
```

## Coding guidelines

- Keep changes minimal and focused.
- Prefer compatibility with the Go implementation when touching storage/WAL.
- Add tests for behavioral changes (especially persistence, WAL replay, and key handling).

## Submitting changes

- Open a PR with a clear description of the behavior change.
- Include a short note in `CHANGELOG.md` under **Unreleased** when user-visible behavior changes.

