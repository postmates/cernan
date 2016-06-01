.PHONY: install test test_unit test_integration

ENV=env
PIP=$(ENV)/bin/pip
PYTEST=$(ENV)/bin/py.test

dist:
	mkdir -p dist

target/release/statsd: src/*.rs
	cargo build --release

install: dist target/release/statsd
	mv target/release/statsd dist/


target/debug:
	mkdir -p target/debug

target/debug/statsd: target/debug src/*.rs
	cargo build

$(ENV):
	virtualenv $(ENV)
	$(PIP) install -r tests/requirements.txt


test: test_unit test_integration

test_unit:
	cargo test

test_integration: target/debug/statsd $(ENV)
	$(PYTEST) tests/
