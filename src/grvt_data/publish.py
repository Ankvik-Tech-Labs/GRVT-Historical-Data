"""GitHub releases: sealed bytes first; a verified public body commits them."""

import hashlib
import json
import os
import re
import selectors
import shutil
import subprocess
import tempfile
import time
from datetime import date, datetime
from pathlib import Path

from .archive import bundle_path, verify_journal
from .config import Config, remaining_seconds
from .storage import publication_db

POINTER = re.compile(
    r"^Active revision: ([1-9][0-9]*), (manifest-rev[0-9]+\.json), sha256 ([0-9a-f]{64})$",
    re.MULTILINE,
)


def _pointer(release):
    matches = list(POINTER.finditer(release.get("body") or ""))
    match = matches[0] if len(matches) == 1 else None
    if match and match[2] == f"manifest-rev{int(match[1]):02d}.json":
        return int(match[1]), match[2], match[3]
    return None


def _fds():
    value = os.environ.get("GRVT_PUBLICATION_LOCK_FD")
    return (int(value),) if value is not None else ()


class GitHub:
    """Host-side gh owns authentication. No token is read or sent by this code."""

    def __init__(self, config: Config, *, deadline=None):
        self.config, self.deadline = config, deadline
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", config.repo):
            raise ValueError("invalid GitHub repository")
        self.base = f"repos/{config.repo}"

    def _run(self, args, *, payload=None, absent=False, json_output=True, maximum=None):
        with tempfile.TemporaryDirectory(prefix="grvt-gh-") as temp:
            command = ["gh", *args]
            if payload is not None:
                path = os.path.join(temp, "request.json")
                with open(path, "w") as stream:
                    json.dump(payload, stream)
                command += ["--input", path]
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=remaining_seconds(
                    self.deadline,
                    self.config.request_timeout_seconds if maximum is None else maximum,
                ),
                pass_fds=_fds(),
                check=False,
            )
        if result.returncode:
            message = result.stderr.decode(errors="replace")
            if absent and "(HTTP 404)" in message:
                return None
            raise RuntimeError(
                f"GitHub command failed ({result.returncode}): {message[:500]}"
            )
        if not json_output or not result.stdout.strip():
            return None
        if "--paginate" in args:
            # gh 2.46 has --paginate but no --slurp: stdout is consecutive
            # complete JSON arrays, not one JSON document or JSON lines.
            decoder = json.JSONDecoder()
            remaining = result.stdout.decode().lstrip()
            pages = []
            while remaining:
                page, end = decoder.raw_decode(remaining)
                if not isinstance(page, list):
                    raise TypeError("expected a JSON array from paginated REST list")
                pages.append(page)
                remaining = remaining[end:].lstrip()
            return pages
        return json.loads(result.stdout)

    def release(self, day):
        release = self._run(
            ["api", f"{self.base}/releases/tags/data-{day}"], absent=True
        )
        if release is None:
            # A draft does not create its Git tag until publication. GitHub's
            # by-tag endpoint can therefore return 404 for an existing draft.
            pages = self._run(
                ["api", f"{self.base}/releases?per_page=100", "--paginate"]
            )
            matches = [
                row
                for page in pages
                for row in page
                if row["tag_name"] == f"data-{day}"
            ]
            if len(matches) > 1:
                raise ValueError("multiple releases for date; fail closed")
            release = matches[0] if matches else None
        if release is not None:
            pages = self._run(
                [
                    "api",
                    f"{self.base}/releases/{release['id']}/assets?per_page=100",
                    "--paginate",
                ]
            )
            release["assets"] = [asset for page in pages for asset in page]
        return release

    def create(self, day):
        return self._run(
            ["api", "--method", "POST", f"{self.base}/releases"],
            payload={
                "tag_name": f"data-{day}",
                "name": f"GRVT data {day}",
                "draft": True,
            },
        )

    def upload(self, release, path):
        # No --clobber: overwriting an immutable asset is never an option.
        return self._run(
            [
                "release",
                "upload",
                release["tag_name"],
                str(path),
                "--repo",
                self.config.repo,
            ],
            json_output=False,
            maximum=self.config.per_date_deadline_seconds,
        )

    def delete(self, asset):
        self._run(
            ["api", "--method", "DELETE", f"{self.base}/releases/assets/{asset['id']}"]
        )

    def patch(self, release, *, body=None, draft=None):
        # Preserve a draft's requested tag even before that Git ref exists.
        # Omitting it on PATCH can replace it with GitHub's untagged-* identity.
        payload = {"tag_name": release["tag_name"]}
        if body is not None:
            payload["body"] = body
        if draft is not None:
            payload["draft"] = draft
        return self._run(
            ["api", "--method", "PATCH", f"{self.base}/releases/{release['id']}"],
            payload=payload,
        )

    def hash(self, asset):
        digest = asset.get("digest")
        if isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            return digest[7:]
        # Stream binary stdout directly into the digest. No second local copy,
        # no line buffering, no blocking read that hides a trickle overrun.
        command = [
            "gh",
            "api",
            f"{self.base}/releases/assets/{asset['id']}",
            "-H",
            "Accept: application/octet-stream",
        ]
        # A finite command budget also applies when no worker deadline exists.
        end = time.monotonic() + remaining_seconds(
            self.deadline, self.config.per_date_deadline_seconds
        )
        with tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=errors, pass_fds=_fds()
            )
            digest = hashlib.sha256()
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            try:
                while selector.get_map():
                    remaining_seconds(self.deadline)
                    budget = end - time.monotonic()
                    if budget <= 0:
                        raise TimeoutError("remote hash command exceeded budget")
                    for key, _ in selector.select(min(0.1, budget)):
                        chunk = os.read(key.fd, 1024**2)
                        if chunk:
                            digest.update(chunk)
                        else:
                            selector.unregister(key.fileobj)
                process.wait(
                    timeout=remaining_seconds(
                        self.deadline, max(0.001, end - time.monotonic())
                    )
                )
                if process.returncode:
                    raise RuntimeError("GitHub asset stream failed")
            finally:
                selector.close()
                process.stdout.close()
                if process.poll() is None:
                    process.kill()
                    process.wait()
            return digest.hexdigest()


def _manifest_asset(row):
    journal = json.loads(row["journal_json"])
    return next(a for a in journal["assets"] if a["dataset"] == "manifest")


def _expected(manifest, manifest_asset):
    assets = [
        *manifest["depth"]["assets"],
        *manifest["trades"]["assets"],
        manifest_asset,
    ]
    result = {}
    for asset in assets:
        if (
            asset["name"] in result
            and result[asset["name"]]["sha256"] != asset["sha256"]
        ):
            raise ValueError("cumulative manifest asset identity collision")
        result[asset["name"]] = asset
    return result


def _matches(release, revision, asset):
    return (
        release is not None
        and not release["draft"]
        and _pointer(release) == (revision, asset["name"], asset["sha256"])
    )


def _commit(db, day, revision):
    with db:
        if db.execute(
            "SELECT count(*) FROM parts WHERE day=? AND revision=? AND verified=0",
            (str(day), revision),
        ).fetchone()[0]:
            raise ValueError("cannot commit unverified parts")
        db.execute(
            "UPDATE revisions SET status='committed',remote_verified=1 WHERE day=? AND revision=?",
            (str(day), revision),
        )


def publish_revision(
    config: Config, day: date, revision: int, *, deadline=None, github=None
) -> dict:
    github = github or GitHub(config, deadline=deadline)
    db = publication_db(config)
    try:
        row = db.execute(
            "SELECT * FROM revisions WHERE day=? AND revision=?", (str(day), revision)
        ).fetchone()
        if row is None or row["status"] == "sealing":
            raise ValueError("publication requires a sealed revision")
        manifest = json.loads(row["manifest_json"])
        asset = _manifest_asset(row)
        expected = _expected(manifest, asset)
        journal = json.loads(row["journal_json"])
        own = {a["name"]: a for a in journal["assets"]}
        release = github.release(day)
        pointer = _pointer(release) if release else None
        protected = set()
        for previous in db.execute(
            "SELECT journal_json FROM revisions WHERE day=? AND (status='committed' OR commit_attempted=1)",
            (str(day),),
        ):
            protected.update(a["name"] for a in json.loads(previous[0])["assets"])
        if release and not release["draft"]:
            if pointer is None:
                raise ValueError(
                    "public release has unknown body; cannot establish asset ownership"
                )
            if pointer[0] > revision:
                later = db.execute(
                    "SELECT * FROM revisions WHERE day=? AND revision=? AND status='committed'",
                    (str(day), pointer[0]),
                ).fetchone()
                if (
                    row["status"] == "committed"
                    and later
                    and _matches(release, pointer[0], _manifest_asset(later))
                ):
                    return {
                        "day": str(day),
                        "revision": revision,
                        "status": "committed",
                        "active_revision": pointer[0],
                    }
                raise ValueError("refusing to roll back public manifest pointer")
            current = db.execute(
                "SELECT * FROM revisions WHERE day=? AND revision=?",
                (str(day), pointer[0]),
            ).fetchone()
            if (
                current is None
                or not current["journal_json"]
                or not _matches(release, pointer[0], _manifest_asset(current))
            ):
                raise ValueError("unknown public manifest pointer; fail closed")
            protected.update(
                _expected(
                    json.loads(current["manifest_json"]), _manifest_asset(current)
                )
            )
        already_public = _matches(release, revision, asset)
        if not already_public and row["status"] == "committed":
            raise ValueError("committed revision lost its public body pointer")
        if not already_public:
            verify_journal(bundle_path(config, day, revision), journal, deadline)
        assets = {a["name"]: a for a in release["assets"]} if release else {}
        if not 1 <= config.release_asset_cap <= 1000:
            raise ValueError("release_asset_cap must be 1..1000")
        if len(assets) + len(set(expected) - set(assets)) > config.release_asset_cap:
            raise ValueError("GitHub release asset cap exceeded; no bytes pruned")
        if release is None:
            release = github.create(day)
        for name, record in expected.items():
            remaining_seconds(deadline)
            remote = assets.get(name)
            if remote is None:
                if already_public or name in protected or name not in own:
                    raise ValueError(f"referenced asset missing: {name}")
                github.upload(
                    release, bundle_path(config, day, revision) / record["path"]
                )
                release = github.release(day)
                assets = {a["name"]: a for a in release["assets"]}
                remote = assets.get(name)
                if remote is None:
                    raise ValueError(f"upload did not create asset: {name}")
            valid = (
                remote["size"] == record["size"]
                and github.hash(remote) == record["sha256"]
            )
            if not valid:
                if (
                    already_public
                    or row["status"] == "committed"
                    or name in protected
                    or name not in own
                ):
                    raise ValueError(
                        f"immutable referenced asset hash mismatch: {name}"
                    )
                # Re-observe the pointer BEFORE deleting an orphan. A successful
                # remote commit followed by process death must never enter here.
                observed = github.release(day)
                if not observed["draft"] and _pointer(observed) != pointer:
                    raise ValueError(
                        "public pointer changed during upload; refusing orphan repair"
                    )
                github.delete(remote)
                github.upload(
                    release, bundle_path(config, day, revision) / record["path"]
                )
                release = github.release(day)
                assets = {a["name"]: a for a in release["assets"]}
                remote = assets[name]
                if (
                    remote["size"] != record["size"]
                    or github.hash(remote) != record["sha256"]
                ):
                    raise ValueError(f"reuploaded asset hash mismatch: {name}")
            if name in own:
                with db:
                    db.execute(
                        "UPDATE parts SET remote_id=?,verified=1 WHERE day=? AND revision=? AND name=?",
                        (remote["id"], str(day), revision, name),
                    )
        if not already_public:
            # Recheck sealed bytes immediately before changing the public pointer.
            verify_journal(bundle_path(config, day, revision), journal, deadline)
            body = (
                f"Active revision: {revision}, {asset['name']}, sha256 {asset['sha256']}\n"
                f"Ticks: {'full day' if manifest['trades']['full_day_complete'] else 'incomplete/provisional'}; "
                f"depth: {manifest['depth']['status']} (received_ns; trades use exchange time)."
            )
            current_release = github.release(day)
            if current_release is None or (
                not current_release["draft"] and _pointer(current_release) != pointer
            ):
                raise ValueError("public pointer changed before commit")
            # A killed/ambiguous remote transition may have become public even
            # if a subsequent outside edit reverts its pointer. Record conservative
            # reference history BEFORE the transition, so retries cannot erase it.
            if not current_release["draft"]:
                with db:
                    db.execute(
                        "UPDATE revisions SET commit_attempted=1 WHERE day=? AND revision=?",
                        (str(day), revision),
                    )
            github.patch(current_release, body=body)
            if current_release["draft"]:
                with db:
                    db.execute(
                        "UPDATE revisions SET commit_attempted=1 WHERE day=? AND revision=?",
                        (str(day), revision),
                    )
                github.patch(current_release, draft=False)
        observed = github.release(day)
        if not _matches(observed, revision, asset):
            raise ValueError("public manifest body pointer not confirmed")
        _commit(db, day, revision)
        return {
            "day": str(day),
            "revision": revision,
            "status": "committed",
            "asset_count": len(observed["assets"]),
            "url": observed.get("html_url"),
        }
    finally:
        db.close()


def prune_verified(
    config: Config, now: datetime, *, deadline=None, github=None
) -> list:
    """Delete exact inputs of proven commits, preserving all small state files.

    `now` is accepted for CLI compatibility; no age-based policy exists. Previous
    revisions keep their historical public-commit proof as the pointer advances.
    Remote hashes were verified at commit; recheck public pointer before cleanup.
    """
    github = github or GitHub(config, deadline=deadline)
    db = publication_db(config)
    removed = []
    try:
        rows = list(
            db.execute(
                "SELECT * FROM revisions WHERE status='committed' AND remote_verified=1 AND pruned=0 ORDER BY day,revision"
            )
        )
        protected_trade_directories = set()
        for unfinished in db.execute(
            "SELECT request_json FROM revisions WHERE status!='committed'"
        ):
            request = json.loads(unfinished[0])
            protected_trade_directories.update(
                Path(s["path"]).resolve().parent
                for s in request["trade_report"]["instruments"].values()
                if s.get("path")
            )
        validated = set()
        for row in rows:
            remaining_seconds(deadline)
            day, revision = date.fromisoformat(row["day"]), row["revision"]
            if day not in validated:
                latest = db.execute(
                    "SELECT * FROM revisions WHERE day=? AND status='committed' ORDER BY revision DESC LIMIT 1",
                    (str(day),),
                ).fetchone()
                if not _matches(
                    github.release(day), latest["revision"], _manifest_asset(latest)
                ):
                    raise ValueError("prune requires verified public manifest pointer")
                validated.add(day)
            if db.execute(
                "SELECT 1 FROM parts WHERE day=? AND revision=? AND verified=0",
                (str(day), revision),
            ).fetchone():
                continue
            manifest = json.loads(row["manifest_json"])
            identities = {
                r[0]
                for r in db.execute(
                    "SELECT identity FROM segments WHERE day=? AND revision=?",
                    (str(day), revision),
                )
            }
            for identity in manifest["inputs"]["segments"]:
                path = config.data_dir / identity
                if identity not in identities or path.suffix != ".frames":
                    raise ValueError("prune segment identity not reserved by revision")
                path.resolve().relative_to(
                    (config.data_dir / "raw" / "depth" / str(day)).resolve()
                )
                if path.exists():
                    path.unlink()
                    removed.append(path)
            for identity in manifest["inputs"]["trades"]:
                path = config.data_dir / identity
                path.resolve().relative_to(
                    (config.data_dir / "trades" / str(day)).resolve()
                )
                if any(
                    directory == path.resolve().parent
                    or directory in path.resolve().parents
                    for directory in protected_trade_directories
                ):
                    continue
                # Canonical Parquet is the durable local target, alongside the
                # release. Page envelopes are transient after verified commit.
                if path.name == "trades.parquet":
                    continue
                if path.exists():
                    path.unlink()
                    removed.append(path)
            bundle = bundle_path(config, day, revision)
            if bundle.exists():
                shutil.rmtree(bundle)
                removed.append(bundle)
            with db:
                db.execute(
                    "UPDATE revisions SET pruned=1 WHERE day=? AND revision=?",
                    (str(day), revision),
                )
        return removed
    finally:
        db.close()


def prune_if_critical(
    config: Config, now: datetime, *, deadline=None, github=None
) -> list:
    """Publication-lock owner calls this from the frequent local status check."""
    from .storage import disk_status

    if disk_status(config.data_dir, config)["cleanup_requested"]:
        return prune_verified(config, now, deadline=deadline, github=github)
    return []
