.PHONY: run-polyhasher run-ffmpeg test clean

TMP := $(shell python3 -c 'import tempfile; print(tempfile.gettempdir())')
SRC := $(CURDIR)/src
EXAMPLE := $(CURDIR)/example
RUN_DIR ?= $(TMP)/tiny-parallel-pipeline-run
POOL ?= 0
VIDEO ?= $(TMP)/video.mp4
YT_DLP ?= $(TMP)/yt-dlp
SOURCE ?= https://www.youtube.com/watch?v=tIeHLnjs5U8

run-polyhasher:
	mkdir -p $(RUN_DIR)
	cd $(RUN_DIR) && time PYTHONPATH=$(SRC) python3 $(EXAMPLE)/yt-dlp-polyhasher.py \
		--pool-workers $(POOL) --video-url $(SOURCE) \
		--video-local-path $(VIDEO) --yt-dlp-local-bin $(YT_DLP) $(ARGS)
	ls -lht $(VIDEO) $(YT_DLP)

run-ffmpeg:
	mkdir -p $(RUN_DIR)
	cd $(RUN_DIR) && time PYTHONPATH=$(SRC) python3 $(EXAMPLE)/yt-dlp-ffmpeg.py \
		--source $(SOURCE) $(ARGS)
	ls -lht $(RUN_DIR)

test:
	PYTHONPATH=src python3 -m pytest src -v

clean:
	test -n "$(TMP)"
	rm -rf $(RUN_DIR) $(TMP)/tiny_parallel_pipeline $(TMP)/video* $(VIDEO) $(YT_DLP)
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache
