PYTEST = thoa/venv/bin/python -m pytest

test-unit:
	$(PYTEST) tests/unit/ -v

test-integration:
	$(PYTEST) tests/integration/ -v -m "not slow"

test-slow:
	$(PYTEST) tests/integration/ -v -m "slow"

test-nightly:
	$(PYTEST) tests/integration/ -v

tests:
	$(PYTEST) tests/ -v

test-int: test-integration
test-night: test-nightly
