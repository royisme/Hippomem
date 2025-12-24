.PHONY: install test clean run

install:
	uv sync

test:
	uv run pytest

clean:
	rm -rf .venv
	rm -rf __pycache__
	find . -name "*.pyc" -delete

run:
	uv run memlayer
