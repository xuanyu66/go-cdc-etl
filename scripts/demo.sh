#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ETL_URL=${ETL_URL:-http://127.0.0.1:8080}
mkdir -p .local/demo
python3 - <<'PY'
import json
from pathlib import Path
Path('.local/demo/function.json').write_text(json.dumps({'name':'order-transform','source':Path('examples/order-transform.lua').read_text()}))
PY
curl --fail-with-body -sS "$ETL_URL/v1/functions" -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-function-v1' --data-binary @.local/demo/function.json > .local/demo/function-response.json
python3 - <<'PY'
import json
from pathlib import Path
p=json.loads(Path('examples/pipeline.json').read_text());f=json.loads(Path('.local/demo/function-response.json').read_text())
p['spec']['nodes'][1]['lua']['function_ref']=f['function_id']
Path('.local/demo/pipeline.json').write_text(json.dumps(p))
PY
curl --fail-with-body -sS "$ETL_URL/v1/pipelines" -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-pipeline-v1' --data-binary @.local/demo/pipeline.json > .local/demo/pipeline-response.json
PIPELINE_ID=$(python3 -c 'import json; print(json.load(open(".local/demo/pipeline-response.json"))["pipeline_id"])')
curl --fail-with-body -sS "$ETL_URL/v1/pipelines/$PIPELINE_ID/versions" -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-version-v1' -d '{"draft_revision":"1"}' > .local/demo/version.json
python3 - <<'PY'
import json
from pathlib import Path
v=json.loads(Path('.local/demo/version.json').read_text())
Path('.local/demo/start.json').write_text(json.dumps({'version_id':v['version_id']}))
PY
curl --fail-with-body -sS "$ETL_URL/v1/pipelines/$PIPELINE_ID/instances" -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-instance-v1' --data-binary @.local/demo/start.json > .local/demo/instance.json
cat .local/demo/instance.json
