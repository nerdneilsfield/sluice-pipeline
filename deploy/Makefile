ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
IMAGE ?= sluice/worker
TAG ?= 0.1.0

.PHONY: image push up down logs

image:
	docker build -f $(ROOT)/deploy/Dockerfile.worker -t $(IMAGE):$(TAG) $(ROOT)

push:
	docker push $(IMAGE):$(TAG)

up:
	docker compose -f $(ROOT)/deploy/docker-compose.yml up -d

down:
	docker compose -f $(ROOT)/deploy/docker-compose.yml down

logs:
	docker compose -f $(ROOT)/deploy/docker-compose.yml logs -f sluice-worker
