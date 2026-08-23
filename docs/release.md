# Releases

Releases are fully automatic — nobody picks a version number by hand.

## How it works

1. Every push to `main` runs [`.github/workflows/release.yml`](../.github/workflows/release.yml).
2. It computes the next version from [Conventional Commits](https://www.conventionalcommits.org/) since the last tag, using [`svu`](https://github.com/caarlos0/svu):
   - `fix:` → patch bump
   - `feat:` → minor bump
   - `!` after the type (e.g. `feat!:`) or a `BREAKING CHANGE:` footer → major bump
   - `docs:`, `chore:`, `build:`, `ci:`, `test:`, and anything without a recognized prefix → no bump
3. If there's a bump, it creates and pushes the git tag (`vX.Y.Z`), then runs [GoReleaser](https://goreleaser.com) (config: [`.goreleaser.yaml`](../.goreleaser.yaml)) to:
   - Cross-compile `webdav-tunnel` for linux/windows/darwin × amd64/arm64
   - Embed the version into the binary (`-version` flag reads it — see `main.version` in [main.go](../main.go))
   - Package each build as a `.tar.gz` (`.zip` on Windows) with `LICENSE`, `README.md`, and `docs/`
   - Publish a GitHub Release with checksums and a changelog grouped by Features/Fixes/Performance/Other
4. If nothing since the last tag warrants a bump (e.g. only `docs:`/`chore:` commits), the workflow exits without releasing.

The release workflow re-runs `gofmt`/`go vet`/`go test`/`go build` itself before tagging — a broken `main` never gets released, independent of whether [`ci.yml`](../.github/workflows/ci.yml) happened to pass on that push.

## What this means for commit messages

Since the version bump is derived entirely from commit prefixes, use them
deliberately:

- A user-facing bug fix → `fix: ...`
- A new capability → `feat: ...`
- A change that breaks existing config/CLI/wire compatibility → `feat!: ...` or `fix!: ...` (or a `BREAKING CHANGE:` footer) — this is a **major** bump, so use it only for genuine breaking changes (e.g. the scrypt KDF change in [encryption.md](encryption.md) would have warranted this)
- Everything else (docs, refactors, CI, tests) → `docs:`/`refactor:`/`ci:`/`test:`/`chore:` so it doesn't trigger a release on its own

## Manual releases

Normally you don't need this — release timing just follows what merged to
`main`. But if a release-worthy change got committed with the wrong prefix
(e.g. a bug fix committed as `docs:` by mistake) and `auto` detection sees
nothing to release, you can force one:

1. GitHub → **Actions** → **Release** → **Run workflow**.
2. Pick a **bump**: `patch`, `minor`, or `major` (instead of the default `auto`).
3. Run it on `main`.

This skips commit-message detection entirely and tags/releases whatever is
currently on `main` with that bump, using the exact same build-and-publish
steps as an automatic release (still gated on tests passing first).

Equivalent from the CLI: `gh workflow run release.yml -f bump=patch`.
