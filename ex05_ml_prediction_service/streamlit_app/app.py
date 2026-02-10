import sys
from pathlib import Path
import json
from datetime import datetime
import streamlit as st
import pandas as pd

# Autoriser les imports depuis src/
# On remonte d'un cran si l'app est dans un dossier /app
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.predict import predict_one

# Chemins des artefacts
ARTIFACTS_DIR = ROOT / "artifacts"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"
LOCATION_MAP_PATH = ARTIFACTS_DIR / "location_map.json"

@st.cache_resource
def load_spec():
    """Charge la configuration du modèle (colonnes numériques/catégorielles)."""
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))

@st.cache_resource
def load_location_map():
    """Charge le mapping des noms de quartiers vers les IDs."""
    if not LOCATION_MAP_PATH.exists():
        st.error(f"Fichier manquant : {LOCATION_MAP_PATH}. Lancez d'abord loc_map_creation.py")
        return {}
    with open(LOCATION_MAP_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# --- Configuration de la Page ---
st.set_page_config(page_title="NYC Taxi Predictor", page_icon="🚕", layout="wide")
st.title("🚕 Simulateur de tarifs Taxi New-Yorkais")
st.markdown("Prédisez le coût total d'une course en fonction du trajet et de l'heure.")

# Chargement des données
spec = load_spec()
LOCATION_MAP = load_location_map()
REVERSE_LOCATION_MAP = {v: int(k) for k, v in LOCATION_MAP.items()}

# Récupération de l'instant présent pour l'automatisation
now = datetime.now()

# --- Interface Utilisateur ---
with st.form("prediction_form"):
    st.subheader("Paramètres du trajet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Où et Combien ?**")
        pu_zone = st.selectbox("Quartier de départ", options=sorted(list(REVERSE_LOCATION_MAP.keys())))
        do_zone = st.selectbox("Quartier d'arrivée", options=sorted(list(REVERSE_LOCATION_MAP.keys())))
        passengers = st.number_input("Nombre de passagers", min_value=1, max_value=6, value=1)
        distance = st.number_input("Distance estimée (en miles)", min_value=0.1, step=0.1, value=2.5)
        
    with col2:
        st.write("**Quand ?** (Par défaut : Maintenant)")
        # Utilisation des infos systèmes 'now' par défaut
        pickup_date = st.date_input("Date de la course", value=now)
        pickup_time = st.time_input("Heure de la course", value=now)
        
        st.info("""
            **Astuce** : La distance impacte le prix de base, 
            mais l'heure et les quartiers déterminent les suppléments 
            (heures de pointe, frais de congestion, etc.).
        """)

    submitted = st.form_submit_button("Calculer le prix estimé", use_container_width=True)

# Logique de Prédiction
if submitted:
    # Construction du dictionnaire record aligné sur features.py
    # On extrait dynamiquement les features temporelles à partir des inputs
    record = {
        "PULocationID": REVERSE_LOCATION_MAP[pu_zone],
        "DOLocationID": REVERSE_LOCATION_MAP[do_zone],
        "passenger_count": passengers,
        "trip_distance": distance,
        "pickup_hour": pickup_time.hour,
        "pickup_dayofweek": pickup_date.weekday(), # 0=Lundi, 6=Dimanche
        "pickup_month": pickup_date.month
    }

    with st.spinner('Analyse des tarifs en cours...'):
        try:
            # Appel de la fonction de prédiction
            price = predict_one(record)
            
            # Affichage du résultat
            st.divider()
            st.balloons()
            st.metric(label="Estimation du Total Amount", value=f"{price:.2f} $")
            st.caption("Estimation basée sur les données historiques de 2024. Inclut les frais de base et taxes.")
            
        except Exception as e:
            st.error(f"Désolé, une erreur est survenue lors du calcul : {e}")
            st.info("Vérifiez que votre modèle est bien entraîné avec les dernières features.")
