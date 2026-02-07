import sys
from pathlib import Path
import json

import streamlit as st

# allow imports from src/
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.predict import predict_one

ARTIFACTS_DIR = ROOT / "artifacts"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"


@st.cache_resource
def load_spec():
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


st.set_page_config(
    page_title="Exercice 5 – ML Prediction Service",
    layout="centered",
)

st.title("Projet BIG DATA - Prédiction du prix d’une course")

spec = load_spec()
feature_cols = spec["feature_cols"]
categorical_cols = set(spec["categorical_cols"])

st.markdown("### Paramètres de la course")

record = {}

with st.form("prediction_form"):
    for col in feature_cols:
        if col in categorical_cols:
            record[col] = st.text_input(col)
        else:
            record[col] = st.number_input(col, value=0.0)

    submitted = st.form_submit_button("Prédire")

if submitted:
    try:
        yhat = predict_one(record)
        st.success(f"Prix total estimé : **{yhat:.2f} $**")
    except Exception as e:
        st.error(f"Erreur pendant la prédiction : {e}")

