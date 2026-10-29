# Contributing

## Setup

The toolchain is pinned in `mise.toml` (Go 1.27.3). Run every Go command through mise so the pinned version is used:

```bash
mise install
mise exec -- make tools   # golangci-lint and friends into ./bin
sh scripts/setup-precommit.sh
```

## Before sending a change

```bash
mise exec -- make all     # check-format, lint, test
mise exec -- go test -race ./...
```

- Add a CHANGELOG entry under the top `(unreleased)` section, in the same change. Breaking changes go under `### Incompatible` with the migration.
- Update `README.md` or `GUIDE.md` in the same change when you alter what they describe. The README Quick Start is `Example` in `pkg/eventmux/example_test.go`: change both together.
- Package boundaries are enforced by `.go-arch-lint.yml`; update it when you add or remove a package.

## Local infrastructure

`deployments/development/compose.yaml` starts the observability stack (Prometheus, Grafana, Tempo) used by the examples; `example/nats/compose.yaml` starts a NATS server. See [`example/README.md`](example/README.md) for the env vars of the end-to-end example.

## Releasing

Rename the top `(unreleased)` CHANGELOG section to `vX.Y.Z - YYYY-MM-DD`, open a new `(unreleased)` section for the next version, then tag:

```bash
VERSION=v2.0.0
git tag "${VERSION}" && git push origin "${VERSION}"
```
