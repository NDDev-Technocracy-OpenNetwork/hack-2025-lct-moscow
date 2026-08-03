# Security notes

## Scope of this document

This file documents intentionally-unaddressed security findings so that future
maintainers and automated scanners understand why they are exceptions rather
than oversights.

## Vendored upstream code: `dlt/`

The `dlt/` directory is a **vendored copy of the upstream dlt-hub project**
(<https://github.com/dlt-hub>), included as a library/fixture. It is **not**
part of this repository's own application code or CI supply chain.

Because `dlt/` is vendored upstream code, we intentionally do **not** patch its
contents locally. Doing so would diverge from upstream and make future updates
harder. The findings below are tracked here for awareness; permanent fixes
belong upstream.

### Findings inside `dlt/` (vendored — not patched)

- `dlt/.github/workflows/*.yml` contain unpinned / moving-tag GitHub Actions
  references. These workflows are the dlt project's own CI and are **not**
  executed as part of this repository's pipeline (this repo has no top-level
  `.github/workflows/` directory). Summary of action references found:
  - `actions/checkout@master` (moving branch tag) — 13 occurrences
  - `actions/setup-python@v5` — 14 occurrences
  - `actions/checkout@v4` — 2 occurrences
  - plus `@v4`/`@v5`/`@v6` tags on setup-node, configure-pages,
    deploy-pages, upload-pages-artifact, astral-sh/setup-uv, ppremk/lfs-warning
- `dlt/deploy/dlt/Dockerfile.airflow` uses `apache/airflow:2.5.2-python3.9`.
  Python 3.9 reached end-of-life in October 2025. This image is a dlt test
  fixture path; it is not used to build any production artifact of this repo.

### Why not patch

- These files are not part of our CI; they do not run on our events.
- Local edits would be overwritten/conflict on the next vendored upgrade.
- The correct remediation is upstream in the dlt-hub repositories.

If this repo ever imports `dlt/` updates, re-review these paths at that time.
