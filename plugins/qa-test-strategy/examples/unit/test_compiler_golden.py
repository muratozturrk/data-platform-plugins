"""Golden-file tests for the control-plane compiler (T-cp-002)."""
import json, pathlib, pytest
from platform.core.compiler import compile_manifest   # your compiler entry point

CASES = pathlib.Path(__file__).parent / "golden"

@pytest.mark.parametrize("case", sorted(p.name for p in CASES.iterdir() if p.is_dir()))
def test_compiler_output_matches_golden(case, request):
    manifest = json.loads((CASES / case / "input.json").read_text())
    outputs = compile_manifest(manifest, now="2026-11-05T00:00:00Z")   # fixed clock
    for name, content in outputs.items():
        golden = CASES / case / "expected" / name
        if request.config.getoption("--update-golden"):
            golden.parent.mkdir(parents=True, exist_ok=True); golden.write_text(content)
        assert content == golden.read_text(), f"{case}/{name} drifted (run with --update-golden after review)"
