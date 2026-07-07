"""
build_wheel.py -- Build (and optionally publish) the Teradata ETL MCP Server Python wheel.

Builds the Python wheel + sdist for the Teradata ETL MCP Server, with optional version
bumps in pyproject.toml only. Optionally smoke-tests the wheel in a clean venv
and publishes to TestPyPI or production PyPI via twine.

Stages (6 max; some skipped by flags):
    0. (optional, --bump) Bump version in pyproject.toml
    1. Prerequisite check (python build, optional twine, git status report)
    2. Resolve & report version from pyproject.toml
    3. If current version is already published, prompt to bump/continue/abort
    4. Build Python wheel + sdist (python -m build)
    5. Smoke-test wheel in fresh venv                       [skipped by --skip-smoketest]
    6. (optional) Publish wheel + sdist via twine          [opt-in via --publish]

Version types (dev -> rc -> release):
    dev      X.Y.Z.devN
    rc       X.Y.ZrcN
    release  X.Y.Z

Index routing:
    dev      -> TestPyPI
    rc       -> TestPyPI
    release  -> PyPI

Repo argument:
    The first positional argument is the server repo root (default: current
    directory). The script expects this layout:

        <repo>/
            pyproject.toml
            src/...

Examples:
    python build_wheel.py
    python build_wheel.py --bump dev:auto --publish
    python build_wheel.py --bump release:1.0.0 --publish pypi --yes
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


# ---------- pretty output helpers ----------

def banner(stage: str, total: str, title: str) -> None:
    bar = "=" * 70
    print(f"\n{bar}\n[Stage {stage}/{total}] {title}\n{bar}", flush=True)


def info(msg: str) -> None:
    print(f"  - {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"  [OK] {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"  [WARN] {msg}", flush=True)


def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    fail(msg)
    sys.exit(code)


# ---------- subprocess wrapper ----------

def run(
    cmd: list[str],
    cwd: Path,
    label: str,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> None:
    """Run a command, stream output, abort on non-zero exit.

    ``input_text`` (when given) is fed to the child process via stdin.
    """
    info(f"$ {' '.join(cmd)}    (cwd={cwd})")
    start = time.time()
    use_shell = os.name == "nt" and cmd[0] in ("npm", "npx", "code", "vsce")
    completed = subprocess.run(
        cmd if not use_shell else " ".join(_quote(c) for c in cmd),
        cwd=str(cwd),
        shell=use_shell,
        env=env,
        input=input_text,
        text=(input_text is not None),
    )
    elapsed = time.time() - start
    if completed.returncode != 0:
        die(f"{label} failed (exit {completed.returncode}, {elapsed:.1f}s)")
    ok(f"{label} done ({elapsed:.1f}s)")


def _quote(arg: str) -> str:
    return f'"{arg}"' if " " in arg else arg


def _python_module_available(module: str) -> bool:
    return subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
    ).returncode == 0


# ---------- version parsing & formatting ----------

_PYPROJECT_VERSION_LINE = re.compile(
    r'^(?P<prefix>\s*version\s*=\s*")(?P<ver>[^"]+)(?P<suffix>")', re.MULTILINE
)

_PYPROJECT_VER = re.compile(
    r"^(?P<base>\d+\.\d+\.\d+)"
    r"(?:\.dev(?P<dev>\d+)|rc(?P<rc>\d+))?$"
)


def _detect_version_type(ver: str, regex: re.Pattern[str]) -> tuple[str, str, int | None]:
    """Return (type, base, num) where type in {'release','dev','rc'}.
    Aborts on unsupported formats (for example alpha, beta, or post releases)."""
    m = regex.match(ver)
    if not m:
        die(f"Unsupported version format: {ver!r} (expected X.Y.Z[.devN] or X.Y.ZrcN)")
    base = m.group("base")
    if m.group("dev") is not None:
        return "dev", base, int(m.group("dev"))
    if m.group("rc") is not None:
        return "rc", base, int(m.group("rc"))
    return "release", base, None


def _format_pyproject(base: str, type_: str, num: int | None) -> str:
    if type_ == "release":
        return base
    if type_ == "dev":
        return f"{base}.dev{num}"
    if type_ == "rc":
        return f"{base}rc{num}"
    raise ValueError(f"unknown version type: {type_}")


# ---------- version-type to publish-target mapping ----------

def index_for_version_type(version_type: str) -> str:
    return "pypi" if version_type == "release" else "testpypi"


# ---------- bump-spec parsing ----------

_BUMP_RE = re.compile(
    r"^(?P<type>dev|rc|release)(?::(?P<value>auto|\d+|\d+\.\d+\.\d+))?$"
)


def parse_bump_spec(spec: str) -> tuple[str, str | None]:
    """Parse '--bump SPEC'. Returns (type, value)."""
    m = _BUMP_RE.match(spec)
    if not m:
        die(
            f"Invalid --bump spec: {spec!r}. "
            "Use dev:N, dev:auto, rc:N, rc:auto, release, or release:X.Y.Z"
        )
    type_ = m.group("type")
    value = m.group("value")
    if type_ in ("dev", "rc") and value is None:
        die(f"--bump {type_} requires :N or :auto (for example {type_}:67 or {type_}:auto)")
    return type_, value


# ---------- file writers ----------

def write_pyproject_version(path: Path, new_ver: str) -> str:
    text = path.read_text(encoding="utf-8")
    m = _PYPROJECT_VERSION_LINE.search(text)
    if not m:
        die(f"Could not find a `version = \"...\"` line in {path}")
    old = m.group("ver")
    new_text = text[: m.start("ver")] + new_ver + text[m.end("ver") :]
    path.write_text(new_text, encoding="utf-8")
    return old


# ---------- TestPyPI / PyPI index helpers ----------

def fetch_existing_versions(repo: str, package: str = "teradata-etl-mcp-server") -> list[str]:
    base = "https://test.pypi.org/pypi" if repo == "testpypi" else "https://pypi.org/pypi"
    url = f"{base}/{package}/json"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        die(f"Failed to fetch index {url}: HTTP {e.code}")
    except Exception as e:  # pragma: no cover - network failures are environment-specific
        die(f"Failed to fetch index {url}: {e}")
    return list(data.get("releases", {}).keys())


def _try_fetch_existing_versions(
    repo: str,
    package: str = "teradata-etl-mcp-server",
    timeout: float = 5.0,
) -> list[str] | None:
    """Best-effort version of fetch_existing_versions for advisory checks."""
    base = "https://test.pypi.org/pypi" if repo == "testpypi" else "https://pypi.org/pypi"
    url = f"{base}/{package}/json"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        return None
    except Exception:
        return None
    return list(data.get("releases", {}).keys())


def next_free_num(existing: list[str], base: str, type_: str) -> int:
    """Find the next unused dev/rc number for the given X.Y.Z base."""
    if type_ == "dev":
        pat = re.compile(rf"^{re.escape(base)}\.dev(\d+)$")
    elif type_ == "rc":
        pat = re.compile(rf"^{re.escape(base)}rc(\d+)$")
    else:
        return 0
    nums = [int(m.group(1)) for v in existing if (m := pat.match(v))]
    return (max(nums) + 1) if nums else 0


# ---------- pre-stage: bump version ----------

def pre_stage_bump(spec: tuple[str, str | None], pyproject_path: Path) -> tuple[str, str]:
    """Write the bumped version to pyproject.toml. Returns (type, pyver)."""
    type_, value = spec
    bar = "=" * 70
    print(f"\n{bar}\n[Pre-stage] Bump version ({type_})\n{bar}", flush=True)

    if not pyproject_path.is_file():
        die(f"pyproject.toml not found: {pyproject_path}")

    match = _PYPROJECT_VERSION_LINE.search(pyproject_path.read_text(encoding="utf-8"))
    if not match:
        die(f"Could not parse version from {pyproject_path}")
    current_pyver = match.group("ver")
    _, current_base, _ = _detect_version_type(current_pyver, _PYPROJECT_VER)

    new_base = current_base
    new_num: int | None = None

    if type_ == "release":
        if value is not None:
            if not re.match(r"^\d+\.\d+\.\d+$", value):
                die(f"--bump release:{value} must be X.Y.Z form")
            new_base = value
    elif type_ in ("dev", "rc"):
        assert value is not None
        if value == "auto":
            repo = index_for_version_type(type_)
            info(f"querying {repo} for highest existing {type_} number for base {new_base}...")
            existing = fetch_existing_versions(repo)
            new_num = next_free_num(existing, new_base, type_)
            ok(f"next free {type_} number on {repo}: {new_num}")
        else:
            new_num = int(value)

    new_pyver = _format_pyproject(new_base, type_, new_num)
    old_pyver = write_pyproject_version(pyproject_path, new_pyver)

    info(f"pyproject.toml : {old_pyver}  ->  {new_pyver}")
    ok(f"version updated to {new_pyver}")
    return type_, new_pyver


# ---------- stages ----------

TOTAL = "6"


def _check_git_status(repo_root: Path, bumped: bool) -> None:
    """Inspect the working tree and warn about changes that affect the build."""
    if shutil.which("git") is None:
        return
    try:
        result = subprocess.run(
            ["git", "--no-pager", "status", "--porcelain"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return
    if result.returncode != 0:
        return

    raw = [ln for ln in result.stdout.splitlines() if ln.strip()]
    if not raw:
        info("git: working tree clean")
        return

    src_changes: list[str] = []
    manifest_changes: list[str] = []
    artifact_changes: list[str] = []
    untracked: list[str] = []
    other: list[str] = []

    for line in raw:
        if len(line) < 3:
            continue
        status = line[:2]
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        path = path.strip().strip('"').replace("\\", "/")

        if status == "??":
            untracked.append(path)
            continue

        if path.startswith("dist/"):
            artifact_changes.append(path)
            continue

        if path == "pyproject.toml" or path.endswith("/pyproject.toml"):
            manifest_changes.append(path)
            continue

        if path.startswith("src/"):
            src_changes.append(path)
            continue

        other.append(path)

    if src_changes:
        warn(f"git: {len(src_changes)} uncommitted source file(s) -- the built")
        warn("     artifact will reflect THESE edits, not the last commit:")
        for p in src_changes[:5]:
            print(f"         M  {p}")
        if len(src_changes) > 5:
            print(f"         ... and {len(src_changes) - 5} more")
        warn("     Commit before publishing for traceability.")

    if manifest_changes:
        if bumped:
            info(f"git: {len(manifest_changes)} manifest change(s) (from --bump):")
        else:
            info(f"git: {len(manifest_changes)} uncommitted manifest change(s):")
        for p in manifest_changes[:3]:
            print(f"         M  {p}")

    if other:
        info(f"git: {len(other)} other tracked change(s) (config/docs/tests)")
        for p in other[:3]:
            print(f"         M  {p}")
        if len(other) > 3:
            print(f"         ... and {len(other) - 3} more")

    if artifact_changes:
        info(f"git: {len(artifact_changes)} build-artifact change(s) (silent)")

    if untracked:
        info(f"git: {len(untracked)} untracked file(s)")
        for p in untracked[:3]:
            print(f"         ?  {p}")
        if len(untracked) > 3:
            print(f"         ... and {len(untracked) - 3} more")


def stage_prereqs(repo_root: Path, want_publish: bool, bumped: bool) -> None:
    banner("1", TOTAL, "Prerequisite check")
    info(f"python : {sys.executable} ({sys.version.split()[0]})")

    if not _python_module_available("build"):
        die("Python `build` not installed. Run: python -m pip install build")
    info("python -m build : available")

    if want_publish:
        if not _python_module_available("twine"):
            die("Python `twine` not installed. Run: python -m pip install twine")
        info("python -m twine : available")

        user = os.environ.get("TWINE_USERNAME")
        pw = os.environ.get("TWINE_PASSWORD")
        if not user or not pw:
            missing = []
            if not user:
                missing.append("TWINE_USERNAME")
            if not pw:
                missing.append("TWINE_PASSWORD")
            die(
                f"Publishing requires env var(s): {', '.join(missing)}.\n"
                "  For TestPyPI: export TWINE_USERNAME=__token__\n"
                "                export TWINE_PASSWORD=pypi-<your-testpypi-token>\n"
                "  For PyPI:     export TWINE_USERNAME=__token__\n"
                "                export TWINE_PASSWORD=pypi-<your-pypi-token>"
            )
        info(f"TWINE_USERNAME : {user}")
        info(f"TWINE_PASSWORD : <set, {len(pw)} chars>")

    _check_git_status(repo_root, bumped=bumped)
    ok("All prerequisites available")


def stage_resolve_versions(repo_root: Path) -> tuple[str, str]:
    banner("2", TOTAL, "Resolve version")

    pyproject_path = repo_root / "pyproject.toml"
    if not pyproject_path.is_file():
        die(f"pyproject.toml not found at {pyproject_path}")
    py_text = pyproject_path.read_text(encoding="utf-8")
    py_match = _PYPROJECT_VERSION_LINE.search(py_text)
    if not py_match:
        die(f"Could not parse version from {pyproject_path}")
    pyver = py_match.group("ver")

    version_type, base, _ = _detect_version_type(pyver, _PYPROJECT_VER)
    info(f"pyproject.toml : {pyver}   ({version_type}, base {base})")
    ok(f"resolved wheel version {pyver}")
    return pyver, version_type


def maybe_prompt_bump_if_published(
    pyver: str,
    version_type: str,
    pyproject_path: Path,
    auto_yes: bool,
) -> str:
    """If the resolved version is already on its target index, prompt the user."""
    banner("3", TOTAL, "Publishability check")
    target = index_for_version_type(version_type)
    existing = _try_fetch_existing_versions(target)
    if existing is None:
        warn(f"Could not reach {target}; skipping publishability check")
        return pyver
    if pyver not in existing:
        info(f"version {pyver} is free on {target} (publishable with --publish)")
        return pyver

    bumpable = version_type in ("dev", "rc")

    print()
    print("!" * 70)
    print(f"!! Version {pyver} is ALREADY published on {target}.")
    print("!! This build cannot be uploaded -- twine will reject re-uploads.")
    print("!" * 70)

    if auto_yes:
        warn("--yes is set; continuing with the existing version (build will not be uploadable).")
        return pyver

    if bumpable:
        choices = "b/c/a"
        prompt = (
            "\nWhat would you like to do?\n"
            f"  [b] Bump to the next free {version_type} number (recommended)\n"
            "  [c] Continue anyway (build will not be uploadable)\n"
            "  [a] Abort\n"
            f"Choice [{choices}]: "
        )
    else:
        choices = "c/a"
        prompt = (
            "\nWhat would you like to do?\n"
            "  [c] Continue anyway (build will not be uploadable)\n"
            "  [a] Abort  -- then re-run with --bump release:NEW.X.Y to pick a new release\n"
            f"Choice [{choices}]: "
        )

    while True:
        try:
            choice = input(prompt).strip().lower()
        except EOFError:
            die(
                "No interactive input available (stdin closed). "
                "Either pass --yes to continue with the existing version, or "
                f"--bump {version_type}:auto to bump first."
            )
        if choice in ("a", "abort", ""):
            die("Aborted by user.")
        if choice in ("c", "continue"):
            warn(f"Continuing with {pyver}; build will not be uploadable to {target}.")
            return pyver
        if bumpable and choice in ("b", "bump"):
            current = _try_fetch_existing_versions(target) or existing
            base = _detect_version_type(pyver, _PYPROJECT_VER)[1]
            new_num = next_free_num(current, base, version_type)
            new_pyver = _format_pyproject(base, version_type, new_num)
            write_pyproject_version(pyproject_path, new_pyver)
            ok(f"Bumped: pyproject.toml -> {new_pyver}")
            return new_pyver
        print(f"Invalid choice: {choice!r}. Pick one of [{choices}].")


def stage_wheel_build(repo_root: Path, pyver: str) -> list[Path]:
    banner("4", TOTAL, "Build Python wheel + sdist")
    dist = repo_root / "dist"
    dist.mkdir(exist_ok=True)
    for f in dist.glob(f"teradata_etl_mcp_server-{pyver}*"):
        info(f"removing stale: {f.name}")
        f.unlink()
    run([sys.executable, "-m", "build"], repo_root, "python -m build")
    files = sorted(dist.glob(f"teradata_etl_mcp_server-{pyver}*"))
    if len(files) < 2:
        die(
            f"Expected wheel + sdist; found {len(files)} file(s) matching "
            f"teradata_etl_mcp_server-{pyver}* in {dist}"
        )
    for f in files:
        info(f"produced: {f.name} ({f.stat().st_size / 1024:.1f} KiB)")
    ok("Python build done")
    return files


def stage_wheel_smoketest(wheel_path: Path, pyver: str) -> None:
    banner("5", TOTAL, "Smoke-test wheel in fresh venv")
    smoke_dir = Path(tempfile.mkdtemp(prefix="elt-smoketest-"))
    try:
        venv_dir = smoke_dir / "venv"
        run([sys.executable, "-m", "venv", str(venv_dir)], Path.cwd(), "venv create")
        py = venv_dir / ("Scripts" if os.name == "nt" else "bin") / (
            "python.exe" if os.name == "nt" else "python"
        )
        if not py.is_file():
            die(f"venv python not found at {py}")
        run(
            [str(py), "-m", "pip", "install", "--quiet", str(wheel_path)],
            Path.cwd(),
            "pip install wheel",
        )
        info(f"$ {py} -m teradata_etl_mcp_server version")
        result = subprocess.run(
            [str(py), "-m", "teradata_etl_mcp_server", "version"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            die(
                f"Smoke-test 'teradata_etl_mcp_server version' failed (exit {result.returncode}):\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        first_line = (result.stdout or "").splitlines()[0] if result.stdout else ""
        info(f"output : {first_line}")
        if pyver in (result.stdout or ""):
            ok(f"Smoke-test confirms version {pyver}")
        else:
            warn(f"version {pyver} not found in smoke-test output")
    finally:
        shutil.rmtree(smoke_dir, ignore_errors=True)


def confirm_irreversible(pyver: str, target: str, version_type: str) -> None:
    print()
    print("!" * 70)
    print(f"!! IRREVERSIBLE: publishing {version_type} version {pyver} to {target.upper()}")
    print("!! Once uploaded, this filename CANNOT be re-uploaded -- only yanked.")
    print("!" * 70)
    answer = input(
        f"\nType the version exactly to confirm ({pyver}), or anything else to abort: "
    ).strip()
    if answer != pyver:
        die(f"Aborted (you typed {answer!r}, expected {pyver!r})")


def stage_publish(
    target: str,
    dist_files: list[Path],
    pyver: str,
    version_type: str,
    yes: bool,
) -> None:
    banner("6", TOTAL, f"Publish to {target}")

    needs_confirm = (
        (target == "pypi" and version_type == "release")
        or (target == "pypi" and version_type == "rc")
    )
    if needs_confirm and not yes:
        confirm_irreversible(pyver, target, version_type)
    elif (target == "pypi" and version_type == "release") and yes:
        warn(f"Publishing release {pyver} to production PyPI (--yes given)")

    if target == "pypi" and version_type == "dev":
        warn(
            "Publishing a 'dev' version to production PyPI is unusual. "
            "TestPyPI is the conventional target for dev builds."
        )
    if target == "testpypi" and version_type == "release":
        warn(
            "Publishing a release (no .devN/.rcN) to TestPyPI. "
            "Production releases are normally published to PyPI."
        )

    info(f"target : {target}")
    for f in dist_files:
        info(f"upload : {f.name}")

    cmd = [
        sys.executable,
        "-m",
        "twine",
        "upload",
        "--repository",
        target,
        *(str(f) for f in dist_files),
    ]
    run(cmd, Path.cwd(), f"twine upload to {target}")
    ok(f"Published {pyver} to {target}")


# ---------- entrypoint ----------

_SHORT_DESC = """\
Build the Teradata ETL MCP Server wheel + sdist from pyproject.toml. Optionally bump the
version, smoke-test the wheel, and publish to TestPyPI or PyPI.
"""

_EXAMPLES = """\
Examples:
  build_wheel.py                                      build current version
  build_wheel.py /path/to/repo                        build from outside the repo
  build_wheel.py --bump dev:auto --publish            next dev N, build, publish to TestPyPI
  build_wheel.py --bump release:1.0.0 --publish pypi --yes
                                                      production release to PyPI
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description=_SHORT_DESC,
        epilog=_EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("repo", nargs="?", default=".", help="Server repo root (default: cwd)")
    parser.add_argument(
        "--bump",
        help="Bump spec: dev:N|dev:auto|rc:N|rc:auto|release|release:X.Y.Z",
    )
    parser.add_argument(
        "--publish",
        nargs="?",
        const="auto",
        help="Publish via twine (auto|testpypi|pypi)",
    )
    parser.add_argument(
        "--skip-smoketest",
        action="store_true",
        help="Skip wheel smoke-test",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Auto-confirm irreversible operations",
    )
    args = parser.parse_args()

    if args.publish is not None and args.publish not in ("auto", "testpypi", "pypi"):
        die(f"--publish must be testpypi, pypi, or omitted; got {args.publish!r}")

    repo_root = Path(args.repo).resolve()
    if not repo_root.is_dir():
        die(f"Repo path does not exist: {repo_root}")

    pyproject_path = (repo_root / "pyproject.toml").resolve()
    if not pyproject_path.is_file():
        die(
            f"Not a valid Teradata ETL MCP Server repo: {repo_root}\n"
            f"  Missing: {pyproject_path}\n"
            "  Expected layout: <repo>/pyproject.toml"
        )

    overall_start = time.time()
    print(f"Repo root      : {repo_root}")
    print(f"pyproject.toml : {pyproject_path}")

    bumped_type: str | None = None
    if args.bump is not None:
        spec = parse_bump_spec(args.bump)
        bumped_type, _ = pre_stage_bump(spec, pyproject_path)

    want_publish = args.publish is not None

    stage_prereqs(repo_root=repo_root, want_publish=want_publish, bumped=(bumped_type is not None))

    pyver, version_type = stage_resolve_versions(repo_root)
    if bumped_type is not None:
        version_type = bumped_type

    if bumped_type is None:
        pyver = maybe_prompt_bump_if_published(
            pyver,
            version_type,
            pyproject_path,
            auto_yes=args.yes,
        )

    dist_files = stage_wheel_build(repo_root, pyver)
    wheels = [f for f in dist_files if f.suffix == ".whl"]
    if not wheels:
        die(f"No .whl among produced files: {[f.name for f in dist_files]}")
    if args.skip_smoketest:
        banner("5", TOTAL, "Smoke-test wheel in fresh venv")
        warn("skipped via --skip-smoketest")
    else:
        stage_wheel_smoketest(wheels[0], pyver)

    if want_publish:
        target = args.publish
        if target == "auto":
            target = index_for_version_type(version_type)
            info(f"--publish target auto-resolved to {target} (version type: {version_type})")
        stage_publish(target, dist_files, pyver, version_type, args.yes)

    print("\n" + "=" * 70)
    print(f"SUCCESS -- total {time.time() - overall_start:.1f}s")
    print("=" * 70)
    print(f"  Python: {dist_files[0].parent}")
    for f in dist_files:
        print(f"          {f.name}")
    if want_publish:
        target = args.publish if args.publish != "auto" else index_for_version_type(version_type)
        print(f"  Published to {target}")
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        fail("Interrupted by user")
        sys.exit(130)
