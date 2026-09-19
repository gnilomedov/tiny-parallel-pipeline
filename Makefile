.PHONY: install-ephemeral-4-py install-ephemeral-4-uv test run-ffmpeg run-semaphore run-lazy-nap clean

SRC := $(CURDIR)/src

# The interpreter's stdlib dir is the only one every uv env inherits (not site-packages,
# and user-site is off in venvs). Ephemeral: pins one build, so a patch upgrade drops it.
PYTHON := $(shell \
	command -v python 2>/dev/null || \
	command -v python3 2>/dev/null || \
	command -v python3.12 2>/dev/null \
)
install-ephemeral-4-py:
	@test -n "$(PYTHON)" || { echo "No Python interpreter found"; exit 1; }
	@stdlib=$$($(PYTHON) -c 'import os; print(os.path.dirname(os.__file__))'); \
	ln -sfn $(SRC)/tiny_parallel_pipeline $$stdlib/tiny_parallel_pipeline; \
	ls -l $$stdlib/tiny_parallel_pipeline
install-ephemeral-4-uv:
	@stdlib=$$(uv run --no-project --managed-python \
		python -c 'import os; print(os.path.dirname(os.__file__))'); \
	ln -sfn $(SRC)/tiny_parallel_pipeline $$stdlib/tiny_parallel_pipeline; \
	ls -l $$stdlib/tiny_parallel_pipeline

test:
	PYTHONPATH=src python3 -m pytest src -v

# The examples own their own knobs; command line overrides reach the sub-make on their own.
run-ffmpeg run-semaphore run-lazy-nap:
	$(MAKE) -C examples $@

clean:
	d="$${TMPDIR:-/tmp}"; rm -rf $$d/gate* $$d/yt-dlp-ffmpeg-* $$d/naps.zip
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache
