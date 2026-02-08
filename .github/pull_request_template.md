## Summary

<!-- Brief description of what this PR does and why -->

**Story**: <!-- JIRA-ID or link -->
**Type**: feat | fix | test | refactor | docs | chore

---

## G1 Gate Checklist

### Requirements
- [ ] PR linked to Jira story
- [ ] Acceptance criteria present in story
- [ ] Story approved in backlog workflow

### TDD Process
- [ ] Tests written BEFORE implementation (RED phase)
- [ ] Tests pass with implementation (GREEN phase)
- [ ] Code refactored for quality (REFACTOR phase)
- [ ] Commit history shows RED → GREEN → REFACTOR sequence

### Test Coverage
- [ ] Unit tests pass (`make test-unit`)
- [ ] All tests pass (`make test`)
- [ ] Coverage meets target (80%+ overall, 95%+ for critical paths)
- [ ] Test taxonomy markers applied (T1-T8 as appropriate)

#### Applicable test categories:
- [ ] T1 — Logic/unit tests
- [ ] T2 — Contract/schema tests
- [ ] T3 — Data quality tests
- [ ] T4 — Access control/security tests
- [ ] T5 — Idempotency tests
- [ ] T6 — Performance/cost tests
- [ ] T7 — Observability tests
- [ ] T8 — Integration/E2E tests

### Code Quality
- [ ] Lint passes (`make lint`)
- [ ] Type hints on all new functions
- [ ] Docstrings on public functions
- [ ] No debug code or commented-out code
- [ ] No hardcoded credentials or secrets

### Governance Controls
- [ ] Data classification identified (if new data)
- [ ] PII protection implemented (if PII present)
- [ ] Audit columns included (if new curated tables)
- [ ] Access controls defined (if new objects)

### Documentation
- [ ] Architecture docs updated (if architecture changed)
- [ ] ITOH/runbook updated (if operations changed)
- [ ] README or DEVELOPER.md updated (if developer workflow changed)

---

## Notes for Reviewers

<!-- Any context that helps reviewers understand the changes -->
