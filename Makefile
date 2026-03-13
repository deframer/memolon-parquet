.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE download-models download

ifneq ("$(wildcard .env)","")
  include .env
  export $(shell sed 's/=.*//' .env)
endif

all:
	@echo all

clean:
	rm -rf work

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format --check .

type-check:
	uv run mypy .

check: lint format type-check test

fix:
	uv run ruff check --fix .
	uv run ruff format .

sync:
	uv sync

download:
	mkdir -p downloads
	curl -L -o downloads/MTL_grouped.zip "https://zenodo.org/records/3756607/files/MTL_grouped.zip?download=1"
	curl -L -o downloads/MTL_all.zip "https://zenodo.org/records/3756607/files/MTL_all.zip?download=1"

debug:
	uv run python convert.py --type grouped --language de --keeptsv --force

sample:
	python sample.py


