SPEC_URL := https://github.com/mcpwarp/ws-mixer-spec.git
GOSERVER_URL := https://github.com/mcpwarp/ws-mixer-go.git
REPO := $(CURDIR)

# fetch-spec shallow-clones ws-mixer-spec at the tag pinned in spec.pin into
# .spec/ (gitignored), the checkout the fixture tests (tests/conftest.py) and,
# later, the conformance runner read from. No-op if .spec/ is already at that
# pin. Same convention as ws-mixer-rust's and ws-mixer-go's Makefiles and
# ws-mixer-js's scripts/fetch-spec.mjs.
.PHONY: fetch-spec
fetch-spec:
	@pin=$$(cat spec.pin); \
	if [ -f .spec/.pin ] && [ "$$(cat .spec/.pin)" = "$$pin" ]; then \
		echo "fetch-spec: .spec/ already at $$pin"; \
	elif { [ -e .spec ] || [ -L .spec ]; } && [ ! -f .spec/.pin ]; then \
		echo "fetch-spec: .spec exists without a .pin stamp; remove it first" >&2; \
		exit 1; \
	else \
		rm -rf .spec; \
		echo "fetch-spec: cloning ws-mixer-spec@$$pin into .spec/"; \
		git clone --depth 1 --branch "$$pin" $(SPEC_URL) .spec; \
		echo "$$pin" > .spec/.pin; \
	fi

# fetch-goserver does the same for ws-mixer-go at goserver.pin into
# .goserver/, the server side of the pair conformance jobs.
.PHONY: fetch-goserver
fetch-goserver:
	@pin=$$(cat goserver.pin); \
	if [ -f .goserver/.pin ] && [ "$$(cat .goserver/.pin)" = "$$pin" ]; then \
		echo "fetch-goserver: .goserver/ already at $$pin"; \
	elif { [ -e .goserver ] || [ -L .goserver ]; } && [ ! -f .goserver/.pin ]; then \
		echo "fetch-goserver: .goserver exists without a .pin stamp; remove it first" >&2; \
		exit 1; \
	else \
		rm -rf .goserver; \
		echo "fetch-goserver: cloning ws-mixer-go@$$pin into .goserver/"; \
		git clone --depth 1 --branch "$$pin" $(GOSERVER_URL) .goserver; \
		echo "$$pin" > .goserver/.pin; \
	fi

# The conformance build tag compiles in wsmixer.AllowSubfloorTiming, which the
# Go adapter needs.
.PHONY: go-adapter
go-adapter: fetch-goserver
	mkdir -p build
	rm -f build/go-adapter
	cd .goserver && go build -tags conformance -o "$(REPO)/build/go-adapter" ./cmd/conformance-adapter

# The Python adapter needs no build: conformance/adapter/run starts python3 -I on
# src/ directly (WSMIXER_PYTHON picks the interpreter). .spec is its own Go
# module, so the runner runs from inside it with absolute paths back here.
.PHONY: conformance-fixture
conformance-fixture: smoke fetch-spec
	mkdir -p build
	cd .spec && go run ./conformance/runner \
		--spec-root "$(REPO)/.spec" \
		--adapter "python=$(REPO)/conformance/adapter/run" \
		--sdk python --mode fixture \
		--counts "$(REPO)/conformance/COUNTS.json" \
		--report "$(REPO)/build/conformance-fixture.xml"

# --sdk go,python runs both go->go and go->python pairs.
.PHONY: conformance-pair
conformance-pair: smoke go-adapter fetch-spec
	cd .spec && go run ./conformance/runner \
		--spec-root "$(REPO)/.spec" \
		--adapter "go=$(REPO)/build/go-adapter,python=$(REPO)/conformance/adapter/run" \
		--sdk go,python --mode pair \
		--counts "$(REPO)/conformance/COUNTS.json" \
		--report "$(REPO)/build/conformance-pair.xml"

# The runner reports every row as SKIP (and exits 0) when the adapter never
# prints `ready`, so check that directly.
.PHONY: smoke
smoke:
	@line=$$(echo '{"seq":1,"cmd":"shutdown"}' | conformance/adapter/run | head -1); \
	echo "$$line"; \
	echo "$$line" | grep -q '"event":"ready"' || { echo "smoke: adapter did not print ready" >&2; exit 1; }

.PHONY: test
test:
	uv run pytest

.PHONY: lint
lint:
	uv run ruff check .
	uv run ruff format --check .

.PHONY: typecheck
typecheck:
	uv run mypy
