# Task 6 private service IP correction

## Confirmed regression

- Target literal-IP hardening correctly requires a matching
  `target_allow_cidrs` entry even when the exact IP appears in
  `target_allow_hosts`.
- `_service_endpoint` reused that target policy with only an exact-host
  exception. As a result, operator-controlled private literal service bases
  such as `http://127.0.0.1:8080` and `http://10.0.0.8:8080` were rejected
  before `bounded_service_get` could call them.

## Fix

- Private literal service bases now receive an exact `/32` or `/128`
  exception in the service-local policy created by `_service_endpoint`.
- Target policy construction and target request handling are unchanged.
- Existing service safeguards remain in place: public HTTP services require
  HTTPS, private DNS answers require an explicitly private self-host name,
  mixed DNS answers are rejected, redirects are not followed, and protected
  headers remain forbidden.

## TDD and verification

- The focused pre-fix run kept the existing target regression green and
  failed both new private service literal cases at `_service_endpoint`: 1
  passed, 2 failed.
- The focused post-fix run passed all three cases.
- `python3 -m pytest -q tests/unit/test_web_safety.py tests/unit/test_web_providers.py`
  -> 47 passed.
- `python3 -m pytest -q tests/unit`
  -> 195 passed.
- `git diff --check`
  -> passed.
