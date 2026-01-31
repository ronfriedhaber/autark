source .venv/bin/activate;
uv run maturin develop;
uv run ./examples/hello_world.py;
