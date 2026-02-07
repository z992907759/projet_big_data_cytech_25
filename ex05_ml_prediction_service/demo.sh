set -e

set -a
source .env
set +a

echo "== Running tests =="
PYTHONPATH=. uv run pytest -q

echo "== Artifacts =="
ls -lh artifacts/
echo "== Spec head =="
head -n 30 artifacts/feature_spec.json

echo "== Predictions =="
python - <<'PY'
import json, joblib, pandas as pd

spec = json.load(open("artifacts/feature_spec.json"))
pipe = joblib.load("artifacts/model.joblib")

def predict_with(overrides):
    row = {c: 0 for c in spec["feature_cols"]}
    for k,v in overrides.items():
        if k in row:
            row[k] = v
    return float(pipe.predict(pd.DataFrame([row]))[0])

print("Base:", predict_with({}))
print("Distance 8km + 2 pax:", predict_with({"trip_distance": 8.0, "passenger_count": 2}))
PY
