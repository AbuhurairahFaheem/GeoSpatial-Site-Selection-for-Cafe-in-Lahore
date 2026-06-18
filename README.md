# GeoSpatial Site Selection for Cafés in Lahore

An AI-powered, geospatial site-selection engine that scores every neighborhood in Lahore for **cafe viability**, using OpenStreetMap, population, and real-estate signals fused on an H3 hexagonal grid. The system is framed as a **Positive-Unlabeled (PU) learning** problem — we only know where cafes already succeed, not where they would fail — and outputs a ranked, GIS-filtered shortlist of buildable hexes with explainable, per-feature reasoning for every recommendation.

The current scope is **Lahore, Pakistan**, and the **cafe** vertical, but the pipeline (grid → features → PU model → GIS mask → explainability) is designed to generalize to other cities and other commercial verticals (gyms, pharmacies, salons, etc.) by swapping the ground-truth POI layer and amenity weights.

> **Status:** research / portfolio project, single-run reproducible pipeline. Not yet packaged as a service — see [Roadmap](#roadmap).

---

## Table of Contents

- [Why this problem is hard](#why-this-problem-is-hard)
- [System overview](#system-overview)
- [The H3 grid idea](#the-h3-grid-idea)
- [Data collection](#data-collection)
- [Dataset creation & cleaning](#dataset-creation--cleaning)
- [Feature engineering (`build_feature_matrix.py`)](#feature-engineering-build_feature_matrixpy)
- [Modeling: PU Learning with Spy + Bagging](#modeling-pu-learning-with-spy--bagging)
- [GIS spatial masking](#gis-spatial-masking)
- [Explainability + final map](#explainability--final-map)
- [Results](#results)
- [Repository structure](#repository-structure)
- [Getting started](#getting-started)
- [Running the full pipeline](#running-the-full-pipeline)
- [Configuration](#configuration)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why this problem is hard

"Where should I open a cafe in Lahore?" looks like a standard binary classification problem, but it isn't one, for two reasons:

1. **No negative class exists.** We have ~275 confirmed cafe/restaurant locations (positives). We do **not** have a list of "bad locations that were tried and failed" — failed cafes simply vanish from OpenStreetMap, they aren't tagged as failures. Every un-labelled hex in the city is therefore a mix of genuinely bad sites and undiscovered good sites that just don't have a cafe yet.
2. **Success correlates with the very features we're trying to predict.** Cafes cluster near other cafes (commercial agglomeration), so a naive model mostly learns "predict high where cafes already are," which is circular and not useful for *site selection* (finding *new, underserved* opportunity).

This project treats it explicitly as a **PU (Positive-Unlabeled) learning** problem: build only from confirmed positives, statistically separate the unlabelled pool into *reliable negatives* and *likely-positive-but-unlabelled*, and rank every hex in the city by hotspot probability — then apply a hard GIS veto so the ranked list only contains land that can physically host a business.

---

## System overview

```
┌──────────────────┐     ┌───────────────────┐     ┌────────────────────┐
│  1. Data          │     │  2. H3 Grid +      │     │  3. PU Learning     │
│  Collection        │ ──▶ │  Feature Matrix    │ ──▶ │  (XGBoost Spy +     │
│  (OSM, Kontur,     │     │  (build_feature_   │     │  Bagging vs         │
│  Zameen/Graana)    │     │   matrix.py)        │     │  Logistic baseline) │
└──────────────────┘     └───────────────────┘     └─────────┬──────────┘
                                                                │
                          ┌───────────────────┐     ┌──────────▼──────────┐
                          │  5. Explainability  │ ◀── │  4. GIS Spatial      │
                          │  (SHAP) + Folium     │     │  Mask (veto rivers,  │
                          │  interactive map      │     │  graveyards,         │
                          │  (generate_final_     │     │  military zones)     │
                          │   map.py)             │     │  (apply_spatial_     │
                          └───────────────────┘     │   mask.py)            │
                                                       └────────────────────┘
```

Each stage is a standalone, re-runnable script that reads/writes flat CSV/GeoJSON files — there's no hidden state, which makes the pipeline easy to audit, re-run partially, or swap data sources for a different city.

---

## The H3 grid idea

Lahore has no natural "unit of analysis" for site selection — you can't compare raw lat/lon points, and administrative boundaries (union councils) are too coarse and irregular for spatial ML. The project uses **Uber's H3 hexagonal hierarchical grid** to solve this:

- **Why hexagons, not squares:** every hexagon has 6 equidistant neighbors (a square grid has 4 immediate + 4 diagonal neighbors at different distances), which makes neighbor-based spatial operations (density, adjacency, smoothing) mathematically cleaner and avoids the directional bias of square grids.
- **Why H3 specifically:** it's a global, hierarchical indexing system — every hex has a fixed string ID (e.g. `89424d07207ffff`), is reproducible across runs, supports fast parent/child resolution changes, and integrates directly with geopandas/Shapely for polygon operations.
- **Resolution used:** **H3 resolution 9** (~174 m edge length, ~0.105 km² per hex) for the feature matrix and modeling — fine enough to distinguish "this side of the road" from "across the intersection," coarse enough to keep ~3,700 hexes for the whole city (tractable for per-hex KD-tree density queries and per-bag XGBoost training). `config.py` separately defines `H3_RESOLUTION = 8` for coarser, exploratory aggregation used earlier in the project.

**How the grid is built (not pre-defined, but *data-driven*):** rather than tiling the entire Lahore bounding box (which would create tens of thousands of empty hexes with zero signal), `build_feature_matrix.py` seeds the grid only from hexes that actually contain at least one data point across all collected layers (cafes, commercial zones, population, education, parking, roads, luxury amenities, lifestyle POIs, rent listings). This keeps the grid dense with signal — **3,691 hexes** in the current run — instead of being dominated by empty desert/agricultural land outside the urban footprint.

Every hex carries:
- a unique `h3_index`
- a **center latitude/longitude**, used for all distance and density calculations
- a set of **engineered features** (below)
- a **binary label** `is_hotspot` — 1 if a real cafe/restaurant falls inside that hex, 0 otherwise (the "unlabelled" class in the PU framing)

---

## Data collection

All data collection lives in `data/`. Each script is independent, retry-safe, and caches raw API responses so re-runs don't re-hit external services. Primary source: **OpenStreetMap**, queried via `osmnx` and raw **Overpass API** calls (with multi-mirror fallback) for resilience against the public Overpass instance's rate limits and timeouts.

| Script | Produces | Source | Notes |
|---|---|---|---|
| `extract_lahore_data.py` | `lahore_population.csv`, `lahore_commercial.csv` | Kontur Population (H3-gridded, ~400 m) + OSM Overpass | Population: clips Kontur's Pakistan GeoPackage to Lahore's bbox via reprojected EPSG:3857 coordinates. Commercial: raw OverpassQL for `landuse=commercial` with 3-server fallback + JSON caching. |
| `foods.py` | `lahore_cafes_restaurants.csv` | OSM (`amenity`: cafe, restaurant, fast_food, food_court) | **This is the ground-truth positive class** for the PU model. |
| `luxuries.py` | `lahore_luxuries_amenities.csv` | OSM | 12 weighted amenity layers (malls, hotels, salons, gyms, hospitals, schools, banks, jewelry, clothing, cinemas) — weights reflect each amenity's correlation with high-spend foot traffic. |
| `luxuries2.py` | `lahore_lifestyle_combined.csv` | OSM | Second, broader lifestyle layer (sports/padel, fitness, beauty, coworking, supermarkets) added to widen coverage for South Asian OSM tagging conventions, which under-use Western amenity tags. |
| `universities.py` | `lahore_education.csv`, `lahore_parking.csv` | OSM | Universities + colleges (student foot traffic proxy) and parking facilities, fetched together and split by layer. |
| `road.py` / `roads.py` / `extract_roads.py` | `lahore_weighted_roads.csv`, `lahore_roads.gpkg`, `lahore_road_nodes.csv` | OSM road network via `osmnx.graph_from_bbox` | Three iterations of road extraction: a lightweight node-only Overpass query (used in the final feature matrix), a full graph-to-GeoDataFrame pipeline with road-class weighting (motorway/primary/secondary/etc.) and segment-importance scoring (`length × commercial_weight`), and a GeoPackage export for GIS work. |
| `zameen.py` | `lahore_graana_clean.csv`, `lahore_rent_final.csv` (+ raw files in `data/no-use/`) | Zameen.com / Graana.com listings (via Kaggle mirrors + direct scraping attempts) | Real-estate rent data used as a **commercial-cost / neighborhood-wealth proxy**. Contains the project's messiest data wrangling — price strings ("PKR45 Thousand", "5.95 Lakh") and area units (Marla, Kanal) are parsed into a normalized `rent_per_sqft`. The file retains earlier, commented-out iterations as a visible record of the cleaning approach. |
| `gencoding.py` | geocoded rent listings | Nominatim (OpenStreetMap geocoder) | Custom polite-geocoding wrapper: randomized user-agents, 2–4 s jittered delays, and automatic 60 s cooldown + retry on HTTP 429, to stay within Nominatim's usage policy while geocoding listings that only had a text neighborhood name. |

**Design pattern used throughout:** every collection script (a) projects geometries to `EPSG:3857` to compute true centroids before converting back to `EPSG:4326` for storage, (b) deduplicates spatially by rounding lat/lon to ~11 m precision (4 decimal places) rather than relying on OSM IDs (which differ across node/way/relation types), and (c) logs through a shared rotating-file logger (`utils/logging.py`) so every collection run leaves an auditable trail in `data/logs/`.

---

## Dataset creation & cleaning

Raw OSM/Overpass/Zameen pulls are noisy in predictable ways, so a shared cleaning contract is applied before anything reaches the feature matrix:

- **Flexible schema detection** — `build_feature_matrix.py` doesn't assume exact column names; it pattern-matches case-insensitively across `latitude/lat/y` and `longitude/lon/long/lng/x`, and for the rent file specifically searches for any column containing `rent`, `price`, `value`, or `cost` if no clean numeric rent column is found. This makes the pipeline tolerant to schema drift between data-collection runs.
- **Bounding-box clipping** — every dataset is re-clipped to the Lahore bbox (`south=31.41, west=74.01, north=31.65, east=74.47`) on load, guarding against stray edge points returned by Overpass `out center` queries on ways that cross the boundary.
- **Coordinate validation** — `utils/validation.py` provides `validate_lat_lon()` (asserts lat ∈ [-90,90], lon ∈ [-180,180]) and `no_nulls()` (hard-fails the pipeline if any NaNs survive into the final dataset) as a last-mile safety net.
- **Unit normalization for real estate** — Pakistani property listings mix Marla, Kanal, Lakh, Crore, and Thousand. `zameen.py` standardizes everything to `price_per_sqft` (1 Marla = 225 sqft, 1 Kanal = 4,500 sqft) so rent is comparable across listing types, and classifies each listing as **commercial** (direct cost proxy) vs **residential** (neighborhood wealth proxy).
- **The `no-use/` folder is intentional** — it preserves earlier, larger raw pulls (raw Zameen scrape, unfiltered Graana export, an early 30 MB weighted-roads attempt) that were superseded by cleaner versions but kept for traceability/audit rather than deleted.

---

## Feature engineering (`build_feature_matrix.py`)

This is the core script that turns ten disparate CSVs into one model-ready table, `training_data.csv` (3,691 rows × 10 columns). For every H3 hex it computes:

| Feature | Method |
|---|---|
| `population_in_hex` | Sum of Kontur population counts whose centroid falls in the hex (or point-count proxy if no numeric population column is found). |
| `avg_rent_in_hex` | Mean `price_per_sqft` of rent listings in the hex, with **KD-Tree nearest-neighbor imputation**: hexes with no rent observations inherit the value from their nearest rent-observed hex (in a locally-projected metric space, not raw degrees) rather than being zero-filled, which would have biased the model toward thinking unobserved areas are free. |
| `dist_to_nearest_road` | Haversine-correct distance (computed via an equirectangular projection centered on Lahore's latitude, accurate to <0.5% over the city's ~50 km extent) from the hex center to the nearest road node, via `scipy.spatial.cKDTree`. |
| `commercial_density_1km`, `education_density_1km`, `parking_density_1km`, `lifestyle_density_1km`, `luxury_density_1km` | Count of POIs of each type within a 1 km radius of the hex center, via `cKDTree.query_ball_point`. |
| `is_hotspot` (target) | 1 if a real cafe/restaurant's H3 cell matches this hex, else 0. |

All distance/density math is done in a **local metric projection** (Lahore-centered equirectangular approximation, ~31.53°N), not in raw lat/lon degrees, because 1° of longitude and 1° of latitude represent different physical distances — getting this wrong is one of the most common silent bugs in geospatial ML, and the codebase is explicit about avoiding it (the same care is repeated in the GIS masking stage with a proper UTM projection for area calculations).

---

## Modeling: PU Learning with Spy + Bagging

Implemented in `evaluate_models.py`. Two models are trained side-by-side for direct comparison:

### Baseline — Logistic Regression
Treats every `is_hotspot=0` hex as a confirmed negative (the naive, *wrong* assumption for this problem) with `class_weight="balanced"` as the only concession to the ~12.4:1 class imbalance. This is the "what if we ignored the PU structure entirely" control.

### Proposed — XGBoost PU Spy + Weighted Bagging
A two-stage procedure per training fold:

1. **Spy step.** A random 15% of confirmed positives are secretly relabelled as 0 and mixed into the unlabelled pool ("spies"). A quick XGBoost pass is trained on this intentionally-contaminated dataset. Because the spies are *actually* positive, the lowest probability score any spy receives becomes a **confidence threshold** — any truly-unlabelled hex scoring *below* that threshold is statistically safer to treat as a **Reliable Negative**, since it scored lower than examples we know are real cafes.
2. **Weighted bagging.** For each of `N_BAGS` (default 100) iterations: sample a balanced mini-dataset (all positives + an equal-sized random draw, with replacement, from the Reliable Negative pool), train one XGBoost classifier, and score every hex in the city. The final `xgb_hotspot_probability` is the **mean across all bags** — this ensemble-averages away the variance introduced by which specific negatives got sampled into any one bag.

Both models are evaluated with 5-fold `StratifiedKFold` cross-validation, and **PR-AUC (Average Precision)**, not ROC-AUC, is used as the headline metric — with only 275 positives vs. 3,416 unlabelled, ROC-AUC is misleadingly inflated by trivial true-negative recall, while PR-AUC focuses entirely on how well the model ranks the minority positive class, which is what actually matters for a top-N site recommendation list.

```
xgb_params = n_estimators=200, max_depth=5, learning_rate=0.05,
             subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
             gamma=0.1, reg_alpha=0.1, reg_lambda=1.0
```

Final artefacts: `outputs/predictions.csv` (every hex, both models' probabilities, XGBoost rank), `outputs/model_metrics.csv`, `models/{xgb_pu_model.json, lr_baseline.pkl, lr_scaler.pkl}`, and diagnostic plots (`pr_auc_comparison.png`, `feature_importance.png`).

---

## GIS spatial masking

A ranked list of hexes is not the same as a list of **buildable** sites — a hex can score highly because it sits near a river, a graveyard, or a military-restricted zone, all of which are legally or physically impossible to build a cafe on. `apply_spatial_mask.py` is a hard post-processing veto layer:

1. Take the **top-200** XGBoost-ranked hexes.
2. Convert each H3 index to its true hexagonal polygon (via `h3.cell_to_boundary`).
3. Load `unbuildable_zones.geojson` — OSM-sourced water bodies, cemeteries, and military land for Lahore (queryable via the OverpassQL snippet documented in the script). If this file is missing, the script auto-generates a small placeholder so the pipeline still runs end-to-end without manual GIS work.
4. For each hex, compute the **overlap area as a percentage of hex area** in a proper metric CRS (**UTM Zone 42N / EPSG:32642**, accurate for Lahore) — never in raw WGS-84 degrees, where area comparisons are not valid.
5. **Veto** any hex with >10% overlap (configurable via `--threshold`); its probability is zeroed and it's excluded from the final ranking, with the offending zone's name/type recorded as `veto_reason`.
6. Approved hexes are re-ranked (`final_rank`) and a static overview map (`spatial_mask_map.png`) is rendered showing approved hexes (blue, shaded by probability), vetoed hexes (red, hatched), and the unbuildable zones (grey).

Output: `outputs/masked_predictions.csv` — the dataset every downstream consumer (including the final interactive map) treats as ground truth for "what can actually be recommended."

---

## Explainability + final map

`generate_final_map.py` is the stakeholder-facing deliverable. It produces a single, self-contained HTML file (`outputs/lahore_site_map.html`, no server or build step needed) with three toggleable Folium layers:

1. **Ground Truth Cafes** — all 275 known cafes/restaurants as markers, for visual sanity-checking against the model's recommendations.
2. **AI Recommendations (Top 50 Approved)** — the top 50 GIS-approved hexes, colored by probability (yellow → orange → red), each clickable to open a popup with a **SHAP waterfall-style breakdown**: exactly which features (and by how much) pushed that specific hex's score up or down. SHAP values for the top 50 are precomputed and stored in `outputs/shap_values_top50.csv`.
3. **Choropleth Heatmap** — a city-wide background layer of all approved-hex probabilities, for spatial context around any individual recommendation.

This closes the loop from "black-box ranked list" to "here is exactly why hex X scored higher than hex Y," which is the difference between a model demo and something a business decision-maker could actually act on.

---

## Results

From the current run (`outputs/model_metrics.csv`, 5-fold CV, seed=143):

| Model | PR-AUC (mean ± std) |
|---|---|
| Logistic Regression (Baseline) | 0.309 ± 0.038 |
| XGBoost PU Spy-Bagging (Proposed) | 0.243 ± 0.043 |

**Honest take:** in this run, the simple baseline currently **outperforms** the PU-learning approach on PR-AUC. This is a real, useful finding rather than a flaw to hide — with only 275 positives, the spy-step's Reliable-Negative threshold is estimated from a very small sample (≈41 spies at a 15% spy ratio), which makes the bagging stage sensitive to noise. It also suggests the unlabelled pool may genuinely behave more like true negatives than hidden positives at this label volume — i.e., the dataset may not yet need the full PU machinery to separate cafe-suitable hexes from the rest. This is flagged here explicitly so the comparison plot (`pr_auc_comparison.png`) and metrics file are read in proper context, not as a "the new method wins" result.

What both models agree on (and what the feature-importance / SHAP analysis consistently shows): **`luxury_density_1km` and `lifestyle_density_1km` dominate** the prediction (gain ≈0.49 and ≈0.47 respectively), dwarfing population, rent, road distance, and the other density features (each <0.02 gain). In practice this means: **cafe success in Lahore tracks lifestyle/luxury commercial clustering far more strongly than raw population density or proximity to major roads** — a result that's directionally consistent with how urban café culture actually clusters around malls, gyms, salons, and other discretionary-spend amenities rather than simply "where people live" or "where traffic passes."

The top-ranked approved hex after GIS masking currently sits at `xgb_hotspot_probability ≈ 0.996`, with zero unbuildable-zone overlap.

---

## Repository structure

```
.
├── README.md                          # this file
├── requirements.txt                   # pinned dependencies for the full pipeline
├── LICENSE                            # MIT
├── config.py                          # Global constants: city, H3 resolution, CRS, KNN_K
├── .vscode/
│   └── settings.json
│
├── data/
│   │   # --- Data collection scripts (Phase 1) ---
│   ├── extract_lahore_data.py         # Population (Kontur) + commercial zones (OSM)
│   ├── extract_roads.py               # Lightweight road-node extraction (Overpass)
│   ├── foods.py                       # Ground-truth cafes/restaurants (the PU positive class)
│   ├── gencoding.py                   # Polite Nominatim geocoding wrapper
│   ├── luxuries.py                    # 12 weighted luxury/amenity OSM layers
│   ├── luxuries2.py                   # Broader lifestyle/wellness OSM layers
│   ├── road.py                        # Weighted road network (commercial-value weighting)
│   ├── roads.py                       # Full road graph → GeoPackage export
│   ├── universities.py                # Education + parking POIs
│   ├── zameen.py                      # Real-estate rent scraping/cleaning (Zameen/Graana)
│   ├── unbuildable_zones.geojson      # Rivers / graveyards / military zones — consumed by src/apply_spatial_mask.py
│   │
│   ├── logs/                          # Rotating pipeline run logs, one file per run date
│   │   └── pipeline_20260502.log
│   │
│   ├── no-use/                        # Superseded raw pulls, kept for audit trail (not read by the pipeline)
│   │   ├── graana.csv
│   │   ├── lahore-property-rents-geocoded.csv
│   │   ├── lahore_house_listings_zameen.csv
│   │   ├── lahore_property_signals.csv
│   │   ├── lahore_rent_cleaned.csv
│   │   ├── lahore_rent_final_clean.csv
│   │   ├── lahore_weighted_roads.csv
│   │   ├── raw_data_zameen.csv
│   │   └── zameen_rentals_data.csv
│   │
│   ├── raw/                           # ★ Cleaned, per-layer datasets — the 10 inputs to the feature matrix
│   │   ├── lahore_cafes_restaurants.csv
│   │   ├── lahore_commercial.csv
│   │   ├── lahore_education.csv
│   │   ├── lahore_graana_clean.csv
│   │   ├── lahore_lifestyle_combined.csv
│   │   ├── lahore_luxuries_amenities.csv
│   │   ├── lahore_parking.csv
│   │   ├── lahore_population.csv
│   │   ├── lahore_rent_final.csv
│   │   └── lahore_road_nodes.csv
│   │
│   ├── processed/
│   │   └── training_data.csv          # ★ Model-ready feature matrix (3,691 hexes × 10 columns)
│   │
│   └── utils/
│       ├── geo.py                     # Vectorized haversine, safe centroid projection
│       ├── logging.py                 # Shared rotating-file logger factory
│       └── validation.py              # Lat/lon bounds + null-value guards
│
├── src/
│   │   # --- Modeling + GIS + explainability pipeline (Phases 2–5) ---
│   ├── build_feature_matrix.py        # ★ H3 grid + feature engineering → data/processed/training_data.csv
│   ├── evaluate_models.py             # ★ Logistic baseline vs XGBoost PU Spy+Bagging
│   ├── apply_spatial_mask.py          # ★ GIS veto: unbuildable-zone overlap filtering
│   └── generate_final_map.py          # ★ SHAP + interactive Folium map (final deliverable)
│
├── models/
│   ├── xgb_pu_model.json
│   ├── lr_baseline.pkl
│   └── lr_scaler.pkl
│
├── outputs/
│   ├── predictions.csv                # Every hex, both models' probabilities
│   ├── masked_predictions.csv         # GIS-approved/vetoed final ranking
│   ├── model_metrics.csv              # PR-AUC comparison table
│   ├── shap_values_top50.csv          # Per-feature SHAP attributions, top 50 hexes
│   ├── lahore_site_map.html           # ★ Final interactive deliverable
│   └── plots/
│       ├── pr_auc_comparison.png
│       └── feature_importance.png
│
└── docs/
    ├── GeoSpatial_Site_Selection_Lahore_Cafe.pptx   # Final evaluation slide deck
    └── README.md                                    # earlier draft README — superseded by the root README.md
```

★ = the main pipeline entry points, run in this order.

> **Cleanup flag:** there are currently two README files (`README.md` at the repo root and `docs/README.md`). Worth deleting or clearly retitling the one in `docs/` before submission so a reviewer doesn't land on the wrong one — see [Known limitations](#known-limitations).

---

## Getting started

### Prerequisites
- Python 3.10+ (developed against 3.14 locally; OSMnx/GeoPandas/H3 are the binding constraints — conda is recommended on Windows to avoid GDAL build issues)
- ~1 GB free disk (Kontur's Pakistan population GeoPackage alone is ~200 MB, cached after first download)

### Install

```bash
git clone https://github.com/AbuhurairahFaheem/GeoSpatial-Site-Selection-for-Cafe-in-Lahore.git
cd GeoSpatial-Site-Selection-for-Cafe-in-Lahore

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

**Windows note:** if `geopandas` fails to build via plain pip (a GDAL/Fiona issue), install it via conda first, then install the rest from `requirements.txt`:

```bash
conda install -c conda-forge geopandas osmnx pyogrio
pip install -r requirements.txt
```

---

## Running the full pipeline

Data-collection scripts live directly under `data/` and write their cleaned output into `data/raw/` (superseded or oversized raw pulls land in `data/no-use/` instead, and are not read by anything downstream). The four modeling/GIS/explainability scripts live under `src/` and write into `data/processed/`, `models/`, and `outputs/`. Run everything from the repo root:

```bash
# 1. Data collection (each is independent; safe to re-run individually)
python data/extract_lahore_data.py     # → data/raw/lahore_population.csv, lahore_commercial.csv
python data/foods.py                   # → data/raw/lahore_cafes_restaurants.csv (PU positive class)
python data/luxuries.py                # → data/raw/lahore_luxuries_amenities.csv
python data/luxuries2.py               # → data/raw/lahore_lifestyle_combined.csv
python data/universities.py            # → data/raw/lahore_education.csv, lahore_parking.csv
python data/road.py                    # → weighted road network
python data/extract_roads.py           # → data/raw/lahore_road_nodes.csv
python data/zameen.py                  # → data/raw/lahore_graana_clean.csv, lahore_rent_final.csv
python data/gencoding.py               # geocodes any rent listings missing coordinates

# 2. Build the H3 feature matrix
python src/build_feature_matrix.py     # → data/processed/training_data.csv

# 3. Train + evaluate models
python src/evaluate_models.py                              # default: 100 bags, 15% spy ratio
python src/evaluate_models.py --bags 200 --spy-ratio 0.10   # override hyperparameters
                                                              # → outputs/predictions.csv, outputs/model_metrics.csv, models/*.json / *.pkl

# 4. Apply the GIS spatial mask
python src/apply_spatial_mask.py                            # default: top-200, 10% overlap threshold
python src/apply_spatial_mask.py --top-n 300 --threshold 0.05
                                                              # → outputs/masked_predictions.csv

# 5. Generate the final interactive map
python src/generate_final_map.py                            # → outputs/lahore_site_map.html, outputs/shap_values_top50.csv
```

Open `outputs/lahore_site_map.html` directly in any browser — no server required.

---

## Configuration

Global constants live in `config.py` at the repo root, imported by every script in both `data/` and `src/`:

```python
CITY = "Lahore, Punjab, Pakistan"
H3_RESOLUTION = 8        # coarse exploratory resolution (training uses resolution 9, set in src/build_feature_matrix.py)
MAX_RETRIES = 3
OVERPASS_SLEEP = 2
CRS_WGS84 = "EPSG:4326"
CRS_PROJECTED = "EPSG:3857"
KNN_K = 5
```

Per-script CLI flags (`src/evaluate_models.py`, `src/apply_spatial_mask.py`, `src/generate_final_map.py`) override bagging count, spy ratio, random seed, CV folds, top-N hex count, and GIS overlap threshold without touching code — see each script's `--help`.

Shared helpers used across the `data/` collection scripts live in `data/utils/` (`geo.py`, `logging.py`, `validation.py`); logs from each run are written to `data/logs/`.

---

## Known limitations

- **Small positive class (n=275).** PU-learning's spy-threshold estimation is noisy at this scale, which is the most likely driver of the baseline currently beating the proposed model on PR-AUC (see [Results](#results)).
- **OSM coverage bias.** OpenStreetMap tagging density in Lahore is uneven across neighborhoods; areas with thinner OSM coverage will show artificially low density features regardless of real-world commercial activity.
- **`data/unbuildable_zones.geojson` may be a placeholder** if it wasn't replaced with a real OSM export before a given run — `src/apply_spatial_mask.py` auto-generates a 3-polygon placeholder (a synthetic river strip, graveyard, and military zone) so the pipeline never breaks, but this is explicitly **not** a substitute for a real zoning/land-use dataset in production.
- **Single city, single vertical.** Bounding box, ground-truth POI tags, and luxury-layer weights are all Lahore-cafe-specific; porting to another city or business type requires updating the `LAHORE_BBOX`/`BBOX` constants and the `data/foods.py` ground-truth tags.
- **Two README files currently exist** (root `README.md` and `docs/README.md`) — worth consolidating into one canonical file before final submission so reviewers aren't reading two different versions of the project.

---

## Roadmap

- [x] Add pinned `requirements.txt`
- [ ] Replace placeholder unbuildable-zones data (`data/unbuildable_zones.geojson`) with a verified, sourced GeoJSON for Lahore
- [ ] Investigate PU spy-ratio / reliable-negative threshold sensitivity at low positive-class sizes (k-fold spy ratio sweep)
- [ ] Package the feature-matrix + scoring pipeline behind a lightweight API for on-demand "score this address" queries
- [ ] Generalize the vertical-specific ground-truth layer (`data/foods.py`) into a configurable target POI type
- [ ] Add automated tests around the CRS/projection logic in `data/utils/geo.py` (the most failure-prone part of any geospatial pipeline)
- [ ] Consolidate the duplicate README files (`README.md` vs `docs/README.md`) into one canonical source

---

## License

MIT License — see [LICENSE](LICENSE).
