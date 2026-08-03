.PHONY: tool fmt check test-gincompat tag release-patch release-minor gittag delcommit
#########################################
#### 这是一个标准的发版 Makefile，包含 release-patch / release-minor / gittag / delcommit
########################################

LINT_TARGETS ?= ./...

# ---- 发版配置（各包按需覆盖）----
# BUMP               发版语义级别：patch（默认）/ minor；MAJOR 需建立 /v2 module，脚本拒绝
# RELEASE_REMOTE     发版推送的 remote 名
# COVERAGE_MIN       覆盖率下限（百分比整数），0 表示不检查
# REQUIRE_CHANGELOG  1 表示 CHANGELOG.md 必须有对应版本条目，否则拒绝发版
# EXTRA_TEST_TARGET  发版前额外执行的 make 目标名（如集成测试），留空则跳过
BUMP              ?= patch
RELEASE_REMOTE    ?= gtkit
COVERAGE_MIN      ?= 80
REQUIRE_CHANGELOG ?= 1
EXTRA_TEST_TARGET ?= test-gincompat


tool: ## 只读静态检查（不修改代码，格式化用 make fmt）
	@ echo "▶️ golangci-lint run"
	golangci-lint run $(LINT_TARGETS)
	@ unformatted=$$(gofumpt -l .); \
	if [ -n "$$unformatted" ]; then \
	  echo "✗ 以下文件未按 gofumpt 格式化（运行 make fmt 修复）:"; echo "$$unformatted"; exit 1; \
	fi
	@ echo "✅ golangci-lint run"

fmt: ## 按 gofumpt 格式化代码（唯一允许写文件的格式化入口）
	gofumpt -l -w .

test-gincompat: ## 跑 gin 兼容性回归（独立模块，主模块依赖清单因此不含 gin）
	cd gincompat && go vet ./... && go test -race -count=1 -timeout=5m ./...

## govulncheck 检查漏洞 go install golang.org/x/vuln/cmd/govulncheck@latest
check:
	govulncheck ./...
	gosec ./...
tag:
	@set -e; \
	if [ -n "$$(git status --porcelain)" ]; then \
		echo "✗ 工作区不干净，发版前请先提交或清理："; git status --short; exit 1; \
	fi; \
	echo "▶️ go mod tidy -diff"; go mod tidy -diff; \
	echo "▶️ go vet"; go vet ./...; \
	echo "▶️ golangci-lint run"; golangci-lint run $(LINT_TARGETS); \
	echo "▶️ gofumpt 只读检查"; \
	unformatted=$$(gofumpt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "✗ 以下文件未按 gofumpt 格式化（运行 gofumpt -w . 修复）:"; echo "$$unformatted"; exit 1; \
	fi; \
	echo "▶️ 测试 (race)"; go test -race -count=1 -timeout=5m ./...; \
	if [ -n "$(EXTRA_TEST_TARGET)" ]; then \
		echo "▶️ 额外测试（$(EXTRA_TEST_TARGET)）"; $(MAKE) $(EXTRA_TEST_TARGET); \
	fi; \
	if [ "$(COVERAGE_MIN)" -gt 0 ] 2>/dev/null; then \
		echo "▶️ 覆盖率 ≥ $(COVERAGE_MIN)%"; \
		go test -coverprofile=coverage.out ./... >/dev/null; \
		cov=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); \
		rm -f coverage.out; \
		awk -v c="$$cov" -v m="$(COVERAGE_MIN)" 'BEGIN { if (c+0 < m+0) { printf "✗ 覆盖率 %.1f%% < %s%%\n", c+0, m; exit 1 } printf "✓ 覆盖率 %.1f%%\n", c+0 }'; \
	fi; \
	echo "▶️ benchmark"; go test -bench=. -benchmem -count=3 -run='^$$' ./... >/dev/null; \
	echo "▶️ govulncheck"; govulncheck ./...; \
	echo "▶️ gosec"; gosec -quiet ./...; \
	current=$$(grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' version.go | head -n1 | tr -d 'v'); \
	if [ -z "$$current" ]; then echo "✗ version.go 中未找到版本号"; exit 1; fi; \
	maj=$$(echo $$current | cut -d. -f1); \
	min=$$(echo $$current | cut -d. -f2); \
	patch=$$(echo $$current | cut -d. -f3); \
	case "$(BUMP)" in \
	  patch) new="v$$maj.$$min.$$((patch+1))" ;; \
	  minor) new="v$$maj.$$((min+1)).0" ;; \
	  major) echo "✗ MAJOR 需建立 /v2 模块（module path 加 /v2），仅 bump tag 是错误发布，不由本脚本处理，已拒绝"; exit 1 ;; \
	  *) echo "✗ BUMP 必须为 patch 或 minor（当前: $(BUMP)）"; exit 1 ;; \
	esac; \
	notes=""; \
	if grep -qE "^## \[$$new\] - [0-9]{4}-[0-9]{2}-[0-9]{2}" CHANGELOG.md 2>/dev/null; then \
		notes=$$(awk -v h="## [$$new] -" 'index($$0, h) == 1 { f = 1; next } f && /^## / { exit } f && /^### / { next } f { print }' CHANGELOG.md | sed -e '/./,$$!d'); \
	elif [ "$(REQUIRE_CHANGELOG)" = "1" ]; then \
		echo "✗ CHANGELOG.md 缺少版本条目：## [$$new] - YYYY-MM-DD，发版前请先补齐"; exit 1; \
	fi; \
	printf "Bump (%s): v%s -> %s\n" "$(BUMP)" "$$current" "$$new"; \
	sed -E -i.bak 's/(const Version = ")([^"]+)(")/\1'"$$new"'\3/' version.go; \
	rm -f version.go.bak; \
	git add version.go; \
	git commit -m "chore(release): 发布 $$new"; \
	if [ -n "$$notes" ]; then \
		git tag -a "$$new" -m "$$(printf '版本 %s\n\n主要变更：\n%s\n' "$$new" "$$notes")"; \
	else \
		git tag -a "$$new" -m "$$(printf '版本 %s\n' "$$new")"; \
	fi; \
	git push $(RELEASE_REMOTE) HEAD; \
	git push $(RELEASE_REMOTE) "$$new"; \
	printf "Done: %s\n" "$$new"

release-patch: ## 发布 PATCH 版本（bug 修复 / 文档 / 内部重构）
	@$(MAKE) tag BUMP=patch

release-minor: ## 发布 MINOR 版本（向后兼容的新功能）
	@$(MAKE) tag BUMP=minor

gittag:
	git tag --sort=-version:refname | head -1

## 删除最近一次提交，但保留修改内容
delcommit:
	git reset --soft HEAD~1
