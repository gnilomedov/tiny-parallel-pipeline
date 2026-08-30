.PHONY: run-ffmpeg run-semaphore

SRC := $(CURDIR)/../src
SOURCE ?= https://www.youtube.com/watch?v=tIeHLnjs5U8
WORKERS ?= 8
STRATEGIES ?= to-thread run-in-executor
GATE_TIMEOUT ?= 30

run-ffmpeg:
	time PYTHONPATH=$(SRC) python3 $(CURDIR)/yt-dlp-ffmpeg.py --source $(SOURCE) $(ARGS)

run-semaphore:
	@for s in $(STRATEGIES); do \
		echo; echo "########## strategy: $$s ##########"; \
		PYTHONPATH=$(SRC) timeout $(GATE_TIMEOUT) \
			python3 $(CURDIR)/concurrent-cpp-semaphore.py \
			--strategy $$s --workers $(WORKERS) $(ARGS); \
		rc=$$?; \
		if [ $$rc -eq 124 ]; then echo "[VERDICT] $$s: HUNG, killed at $(GATE_TIMEOUT)s"; \
		elif [ $$rc -eq 0 ]; then echo "[VERDICT] $$s: OK"; \
		else echo "[VERDICT] $$s: FAILED rc=$$rc"; fi; \
	done
