# GeoSpatial Site Selection for Cafés in Lahore

An end-to-end geospatial machine learning pipeline that recommends high-potential
locations for luxury cafés in Lahore, Pakistan — built because no existing dataset or
tool answers this question for the city. The pipeline compiles 10 raw geospatial
sources into a unified hexagonal grid, trains a Positive-Unlabeled (PU) XGBoost model
to rank every location by hotspot probability, filters out physically unbuildable
sites with a GIS spatial mask, and outputs an interactive, explainable map of the
top 50 recommended sites.

**Team:** Abuhurairah Faheem (BSCS23077) · Muhammad Ammar Bin Talib (BSCS23143)

---

## Problem

Site selection for cafés in Lahore is currently driven by manual scouting and
intuition. That approach is slow, doesn't scale across the city, and is biased
toward areas that are already saturated with competitors — it can't surface
unclaimed, high-potential locations no one has tried yet. This project replaces that
guesswork with a trained model that scores every part of the city.

## Architecture

```
10 Raw Sources (OSM, Kontur, Graana, ...)
        │
        ▼
 H3 Hexagonal Grid (Resolution 9, ~174m cells)  ──▶  data/processed/training_data.csv
        │
        ▼
 PU-Learning XGBoost  (Spy + Weighted Bagging vs. Logistic Regression baseline)
        │
        ▼
 GIS Spatial Veto  (drops hexes overlapping water/graveyards/military zones >10%)
        │
        ▼
 SHAP Explainer  +  Folium Interactive Map  ──▶  outputs/lahore_site_map.html
```

## Repository Structure

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── config.py                  # LAHORE_BBOX, CBD coordinate, shared file paths
│
├── data/
│   ├── raw/                   # the 10 original source CSVs
│   ├── processed/
│   │   └── training_data.csv  # final 3,691-row feature matrix
│   └── unbuildable_zones.geojson
│
├── src/
│   ├── build_feature_matrix.py    # Step 1 — builds the H3 feature matrix
│   ├── evaluate_models.py         # Step 2 — trains LR baseline + XGBoost PU model
│   ├── apply_spatial_mask.py      # Step 3 — GIS veto on top-200 candidates
│   └── generate_final_map.py      # Step 4 — SHAP + Folium interactive map
│
├── models/
│   ├── xgb_pu_model.json
│   └── lr_baseline.pkl
│
├── outputs/
│   ├── predictions.csv
│   ├── masked_predictions.csv
│   ├── shap_values_top50.csv
│   ├── plots/
│   │   ├── pr_auc_comparison.png
│   │   └── feature_importance.png
│   └── lahore_site_map.html       # final interactive deliverable
│
├── docs/
│   ├── Group11_Proposal.docx
│   └── Group11_Final_Report_Draft.docx
│
└── presentation/
    └── Group11_FinalEval_5Slide.pptx
```

## Setup

```bash
git clone https://github.com/AbuhurairahFaheem/GeoSpatial-Site-Selection-for-Cafe-in-Lahore.git
cd GeoSpatial-Site-Selection-for-Cafe-in-Lahore
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

If `geopandas` fails to install via pip on Windows (a common GDAL/Fiona issue), use
conda instead:

```bash
conda install -c conda-forge geopandas
pip install -r requirements.txt
```

## Running the Pipeline

Run the four scripts in order from the repo root:

```bash
python src/build_feature_matrix.py     # -> data/processed/training_data.csv
python src/evaluate_models.py          # -> outputs/predictions.csv, models/*.json/*.pkl
python src/apply_spatial_mask.py       # -> outputs/masked_predictions.csv
python src/generate_final_map.py       # -> outputs/lahore_site_map.html
```

Open `outputs/lahore_site_map.html` in any browser to explore the final
recommendations.

## Dataset

| Metric | Value |
|---|---|
| Raw sources merged | 10 (OpenStreetMap, Kontur population, Graana real estate, and others) |
| Spatial grid | Uber H3, Resolution 9 (~174m hexagon edge) |
| Total hexagons | 3,691 |
| Known cafés (positive labels) | 275 |
| Unlabeled hexagons | 3,416 (class ratio ≈ 1:12.4) |
| Engineered features | 11 — population, rent (IDW-imputed), road distance, 5 density-within-1km features, hex centroid lat/lon, distance to CBD |

## Results

| Model | PR-AUC (5-fold mean) |
|---|---|
| Logistic Regression (baseline) | ≈ 0.31 |
| XGBoost — PU Spy + Bagging (proposed) | ≈ 0.24 |

The proposed model's lower PR-AUC is expected, not a defect: PR-AUC treats every
unlabeled hex as a confirmed negative, so it penalizes the model whenever it
correctly identifies a hidden hotspot — the exact behavior we want (see Elkan & Noto,
2008, on evaluating Positive-Unlabeled classifiers). The model is validated
qualitatively instead, via SHAP explanations per recommended hex and the GIS spatial
mask correctly vetoing a top-ranked candidate that overlapped Pir Makki Graveyard.

## Future Work

- Temporal foot-traffic signals (time-of-day, weekday vs. weekend)
- Spatial cannibalization modeling to avoid recommending adjacent, competing sites
- Portability to other cities (the pipeline is parameterized by bounding box and CBD coordinate)

## License

MIT — see [LICENSE](./LICENSE).
