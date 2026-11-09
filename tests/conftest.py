import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# pipeline/f1 is not an installed package (it is rendered and synced to S3 as the SDP
# pipeline source), and infra/ is the CDK app root, so tests add both to sys.path.
for path in (REPO_ROOT / "pipeline" / "f1", REPO_ROOT / "infra", REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
