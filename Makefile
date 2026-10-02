.PHONY: install test data-synth data train up down init simulate loadtest step-load summarize
install:
	pip install -e ".[dev,load]"
test:
	pytest -q
data-synth:            # smoke-test data ONLY, never for resume numbers
	python -m sentinelstream.prepare --synthetic
data:                  # real IEEE-CIS (needs data/raw/*.csv from Kaggle)
	python -m sentinelstream.prepare --raw-dir data/raw
train:
	python -m sentinelstream.train --target-precision 0.10
up:
	docker compose up -d --build
down:
	docker compose down -v
init:
	docker compose run --rm init
simulate:
	python -m sentinelstream.simulator --rate 2000 --limit 100000
loadtest:
	locust -f loadtest/locustfile.py --host http://localhost:8080 --headless -u 100 -r 50 -t 60s --csv results/locust
step-load:
	STEP_LOAD=1 locust -f loadtest/locustfile.py --host http://localhost:8080 --headless --csv results/locust_step
summarize:
	python scripts/summarize_results.py --label "$(LABEL)"
