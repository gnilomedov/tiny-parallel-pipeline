.PHONY: run-ffmpeg run-semaphore run-lazy-nap

SRC := $(CURDIR)/../src
SOURCE ?= https://www.youtube.com/watch?v=tIeHLnjs5U8
WORKERS ?= 8
STRATEGIES ?= to-thread run-in-executor
GATE_TIMEOUT ?= 30
NAPS_ZIP ?= /tmp/naps.zip

run-ffmpeg:
	time PYTHONPATH=$(SRC) python3 $(CURDIR)/yt-dlp-ffmpeg.py --source $(SOURCE) $(ARGS)

run-semaphore:
	@for s in $(STRATEGIES); do \
		echo; echo "########## strategy: $$s ##########"; \
		time PYTHONPATH=$(SRC) timeout $(GATE_TIMEOUT) \
			python3 $(CURDIR)/concurrent-cpp-semaphore.py \
			--strategy $$s --workers $(WORKERS) $(ARGS); \
		rc=$$?; \
		if [ $$rc -eq 124 ]; then echo "[VERDICT] $$s: HUNG, killed at $(GATE_TIMEOUT)s"; \
		elif [ $$rc -eq 0 ]; then echo "[VERDICT] $$s: OK"; \
		else echo "[VERDICT] $$s: FAILED rc=$$rc"; fi; \
	done

# Twice over one archive: the second pass reads back what the first one slept for.
run-lazy-nap:
	@rm -f $(NAPS_ZIP)
	@for pass in cold warm; do \
		echo; echo "########## pass: $$pass ##########"; \
		time PYTHONPATH=$(SRC) python3 \
			$(CURDIR)/concurrent-lazy-nap-timing.py \
			--zip $(NAPS_ZIP) $(ARGS); \
	done
