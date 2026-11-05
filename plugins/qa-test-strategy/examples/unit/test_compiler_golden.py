"""Golden-file tests for the control-plane compiler (T-cp-002).

Point COMPILER_MODULE at your compiler package. Do not name the package `platform`:
it would shadow Python's standard-library `platform` module.
Regenerate golden files after review with: pytest --update-golden
(register the option in conftest.py: parser.addoption("--update-golden", action="store_true")).
"""
import json, pathlib, pytest

COMPILER_MODULE = "hkm_control_plane.compiler"   # your compiler entry point
compile_manifest = pytest.importorskip(COMPILER_MODULE).compile_manifest

CASES = pathlib.Path(__file__).parent / "golden"
CASE_NAMES = sorted(p.name for p in CASES.iterdir() if p.is_dir()) if CASES.is_dir() else []

@pytest.mark.parametrize("case", CASE_NAMES)
def test_compiler_output_matches_golden(case, request):
    manifest = json.loads((CASES / case / "input.json").read_text())
    outputs = compile_manifest(manifest, now="2026-11-05T00:00:00Z")   # fixed clock
    for name, content in outputs.items():
        golden = CASES / case / "expected" / name
        if request.config.getoption("--update-golden", default=False):
            golden.parent.mkdir(parents=True, exist_ok=True); golden.write_text(content)
        assert content == golden.read_text(), f"{case}/{name} drifted (run with --update-golden after review)"
