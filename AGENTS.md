# AGENTS

## Project conventions

- Language: Python (>=3.10)
- Dependencies: keep minimal (`msgpack` required, `dissononce` optional for noise)
- Tests: use `pytest`; run `pytest tests/ -k "not real_daemon"` locally
- Compatibility: wire protocol must stay compatible with the Go SDK in `maxesisnclaw/nexus`
- Commits: use Conventional Commits style
