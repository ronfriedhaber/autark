.PHONY: check test

check:
	cargo check

test:
	. crates/mpera/.venv/bin/activate && \
	cd crates/autark-client && \
	cargo test -- --nocapture
