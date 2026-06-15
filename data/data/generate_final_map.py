"""
generate_final_map.py
=====================
Phase 3 (Final) of the Evaluation Pipeline — SHAP + Interactive Folium Map.

Produces a single self-contained HTML file that stakeholders can open in
any browser with no server, no Node.js, no installation required.

Map layers (toggle-able via LayerControl)
-----------------------------------------
  Layer 1 — Ground Truth Cafes
      275 known cafe/restaurant locations as small blue circle markers.
      Clicking one shows its H3 index.

  Layer 2 — AI Recommendations (Top 50 Approved)
      Top-50 approved hexagons as filled polygons, coloured by probability
      (yellow → orange → red).  Clicking one opens the SHAP dashboard popup.

  Layer 3 — Choropleth Heatmap
      All approved hexagons from masked_predictions.csv, coloured by
      xgb_hotspot_probability.  Background context layer.

Input files
-----------
  outputs/masked_predictions.csv   — from apply_spatial_mask.py
  training_data.csv                — original feature matrix
  outputs/models/xgb_pu_model.json — surrogate XGBoost model for SHAP

Output
------
  outputs/lahore_site_map.html

Install
-------
  pip install folium shap xgboost pandas numpy h3 branca

Usage
-----
  python generate_final_map.py
  python generate_final_map.py --top-n 50 --model outputs/models/xgb_pu_model.json
"""

import os
import json
import argparse
import warnings
import numpy as np
import pandas as pd
import h3
import folium
from folium import plugins
from folium.plugins import MiniMap
import branca.colormap as cm
import shap
import xgboost as xgb

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────
MASKED_PREDS_PATH = os.path.join("outputs", "masked_predictions.csv")
TRAINING_DATA_PATH = "training_data.csv"
MODEL_PATH        = os.path.join("outputs", "models", "xgb_pu_model.json")
OUTPUT_HTML       = os.path.join("outputs", "lahore_site_map.html")

DEFAULT_TOP_N = 50

FEATURE_COLS = [
    "population_in_hex",
    "avg_rent_in_hex",
    "dist_to_nearest_road",
    "commercial_density_1km",
    "education_density_1km",
    "parking_density_1km",
    "lifestyle_density_1km",
    "luxury_density_1km",
]

# Human-readable labels for the popup table
FEATURE_LABELS = {
    "population_in_hex":       "Population (hex)",
    "avg_rent_in_hex":         "Avg Rent (PKR/sqft)",
    "dist_to_nearest_road":    "Dist. to Road (m)",
    "commercial_density_1km":  "Commercial Zones (1km)",
    "education_density_1km":   "Education POIs (1km)",
    "parking_density_1km":     "Parking Facilities (1km)",
    "lifestyle_density_1km":   "Lifestyle POIs (1km)",
    "luxury_density_1km":      "Luxury Amenities (1km)",
}

# Lahore city centre for initial map view
LAHORE_LAT =  31.5204
LAHORE_LON =  74.3587
INITIAL_ZOOM = 12

INDEX_COL   = "h3_index"
TARGET_COL  = "is_hotspot"
H3_RES      = 9


# ─────────────────────────────────────────────────────────────────
#  ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Generate SHAP-powered Folium site map.")
    p.add_argument("--masked",  default=MASKED_PREDS_PATH, help="masked_predictions.csv")
    p.add_argument("--data",    default=TRAINING_DATA_PATH, help="training_data.csv")
    p.add_argument("--model",   default=MODEL_PATH,         help="xgb_pu_model.json")
    p.add_argument("--output",  default=OUTPUT_HTML,         help="Output HTML path")
    p.add_argument("--top-n",   type=int, default=DEFAULT_TOP_N,
                   help="Number of top approved hexes to highlight (default 50)")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────
#  STEP 1 — Load all inputs
# ─────────────────────────────────────────────────────────────────
def load_inputs(args):
    print(f"\n{'='*60}")
    print("STEP 1 — Loading inputs")
    print(f"{'='*60}")

    # ── masked_predictions.csv ────────────────────────────────────
    if not os.path.exists(args.masked):
        raise FileNotFoundError(
            f"'{args.masked}' not found. Run apply_spatial_mask.py first."
        )
    masked = pd.read_csv(args.masked)
    approved = masked[masked["status"] == "Approved"].copy()
    print(f"  Loaded {len(masked):,} total hexes from masked_predictions.csv")
    print(f"  Approved hexes : {len(approved):,}")

    # ── training_data.csv (for feature values + ground-truth cafes) ─
    if not os.path.exists(args.data):
        raise FileNotFoundError(
            f"'{args.data}' not found. This should be your original feature matrix."
        )
    training = pd.read_csv(args.data)
    print(f"  Loaded {len(training):,} rows from training_data.csv")

    # ── XGBoost surrogate model ───────────────────────────────────
    if not os.path.exists(args.model):
        raise FileNotFoundError(
            f"'{args.model}' not found. Run evaluate_models.py first."
        )
    model = xgb.XGBClassifier()
    model.load_model(args.model)
    print(f"  Loaded XGBoost model from '{args.model}'")

    return approved, training, model


# ─────────────────────────────────────────────────────────────────
#  STEP 2 — Prepare top-N hexes with feature values
# ─────────────────────────────────────────────────────────────────
def prepare_top_hexes(approved: pd.DataFrame, training: pd.DataFrame, top_n: int):
    """
    Merge the probability scores (from masked_predictions) with the
    raw feature values (from training_data) for the top-N hexes.

    The masked_predictions CSV contains only h3_index + scores.
    The training_data CSV contains the features. We join on h3_index.
    """
    print(f"\n{'='*60}")
    print(f"STEP 2 — Preparing top {top_n} approved hexes")
    print(f"{'='*60}")

    # Sort approved hexes by probability descending, take top N
    top = (
        approved
        .sort_values("xgb_hotspot_probability", ascending=False)
        .head(top_n)
        .copy()
        .reset_index(drop=True)
    )
    top["map_rank"] = top.index + 1   # 1-based rank for display

    # Merge feature values from training_data
    feature_cols_available = [c for c in FEATURE_COLS if c in training.columns]
    merge_cols = [INDEX_COL] + feature_cols_available

    top = top.merge(
        training[merge_cols],
        on=INDEX_COL,
        how="left",
    )

    missing_features = top[feature_cols_available].isna().sum()
    if missing_features.any():
        print(f"  [WARN] Some hexes had no matching row in training_data:")
        print(missing_features[missing_features > 0])
        for col in feature_cols_available:
            top[col] = top[col].fillna(0.0)

    print(f"  Top-{top_n} probability range: "
          f"{top['xgb_hotspot_probability'].min():.4f} – "
          f"{top['xgb_hotspot_probability'].max():.4f}")

    return top, feature_cols_available


# ─────────────────────────────────────────────────────────────────
#  STEP 3 — Compute SHAP values for top-N hexes
# ─────────────────────────────────────────────────────────────────
def compute_shap_values(model, top_df: pd.DataFrame, feature_cols: list):
    """
    Run shap.TreeExplainer on the surrogate XGBoost model.

    TreeExplainer is the correct explainer for tree-based models —
    it computes exact Shapley values in O(TLD) time (T = trees,
    L = leaves, D = depth) rather than the sampling-based approximation
    used by KernelExplainer.

    Returns
    -------
    shap_df : pd.DataFrame  [top_n rows × len(feature_cols) columns]
              Each cell is the SHAP contribution of that feature for that hex.
              Positive value → pushed probability UP (toward hotspot).
              Negative value → pushed probability DOWN (toward non-hotspot).
    base_value : float  — the model's expected output before feature contributions.
    """
    print(f"\n{'='*60}")
    print("STEP 3 — Computing SHAP values")
    print(f"{'='*60}")

    X = top_df[feature_cols].values.astype(np.float32)

    explainer   = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    # For binary classification XGBoost, shap_values is a 2D array
    # [n_samples × n_features] representing log-odds contributions.
    # If the model returns a list (some versions), take class-1 slice.
    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    shap_df = pd.DataFrame(shap_values, columns=feature_cols)

    base_value = float(explainer.expected_value)
    if isinstance(base_value, (list, np.ndarray)):
        base_value = float(base_value[1] if len(base_value) > 1 else base_value[0])

    print(f"  SHAP matrix shape : {shap_df.shape}")
    print(f"  Base value (E[f(x)]): {base_value:.4f}")
    print(f"  Mean |SHAP| per feature:")
    for col in feature_cols:
        print(f"    {col:35s}: {shap_df[col].abs().mean():.5f}")

    return shap_df, base_value


# ─────────────────────────────────────────────────────────────────
#  STEP 4 — Helper: H3 index → Folium polygon coordinates
# ─────────────────────────────────────────────────────────────────
def h3_to_folium_coords(h3_idx: str):
    """
    Returns coordinates in [[lat, lon], ...] format.

    h3.cell_to_boundary() returns (lat, lon) tuples — Folium expects
    [lat, lon] lists, so we convert and close the polygon ring.
    """
    boundary = h3.cell_to_boundary(h3_idx)   # list of (lat, lon)
    coords   = [[lat, lon] for lat, lon in boundary]
    coords.append(coords[0])                  # close the ring
    return coords


def h3_centroid(h3_idx: str):
    """Return [lat, lon] centroid of an H3 cell."""
    lat, lon = h3.cell_to_latlng(h3_idx)
    return [lat, lon]


# ─────────────────────────────────────────────────────────────────
#  STEP 5 — Build SHAP popup HTML for a single hex
# ─────────────────────────────────────────────────────────────────
def build_shap_popup(row: pd.Series, shap_row: pd.Series,
                     feature_cols: list, base_value: float) -> str:
    """
    Build a rich HTML popup for a top-50 recommendation hex.

    Sections:
      Header       — rank, hex ID, probability score
      Feature vals — raw feature values in a two-column table
      SHAP drivers — top-3 positive and all negative drivers in a table
                     with colour-coded rows (green = pushing up, red = pushing down)
    """
    prob   = row.get("xgb_hotspot_probability", 0.0)
    rank   = int(row.get("map_rank", "?"))
    h3_id  = row[INDEX_COL]

    # ── Probability bar (CSS progress bar, no images required) ───
    bar_pct  = min(int(prob * 100 / max(prob, 0.01) * 100), 100)
    bar_color = (
        "#C0392B" if prob > 0.7 else
        "#E67E22" if prob > 0.4 else
        "#F1C40F"
    )

    # ── Rank badge colour ─────────────────────────────────────────
    badge_color = "#B8860B" if rank <= 10 else "#2E75B6"

    # ── Feature value table ───────────────────────────────────────
    feature_rows_html = ""
    for col in feature_cols:
        val   = row.get(col, 0.0)
        label = FEATURE_LABELS.get(col, col)
        # Format integers cleanly; floats to 2dp
        if isinstance(val, float) and val == int(val):
            val_str = f"{int(val):,}"
        elif isinstance(val, float):
            val_str = f"{val:,.2f}"
        else:
            val_str = str(val)

        feature_rows_html += f"""
          <tr>
            <td style="padding:3px 8px;color:#555;font-size:12px;">{label}</td>
            <td style="padding:3px 8px;font-weight:600;color:#1A2A3A;
                       font-size:12px;text-align:right;">{val_str}</td>
          </tr>"""

    # ── SHAP drivers table ────────────────────────────────────────
    # Sort features by SHAP value descending to find positive/negative drivers
    shap_sorted = shap_row.sort_values(ascending=False)

    positive_drivers = shap_sorted[shap_sorted > 0]
    negative_drivers = shap_sorted[shap_sorted < 0].sort_values(ascending=True)

    # Show top 3 positive and top 3 (most negative) drivers
    top_pos = positive_drivers.head(3)
    top_neg = negative_drivers.head(3)

    shap_rows_html = ""
    for feat, sv in top_pos.items():
        label = FEATURE_LABELS.get(feat, feat)
        shap_rows_html += f"""
          <tr style="background:#EAF7EC;">
            <td style="padding:3px 8px;font-size:12px;color:#1A6B3C;">▲ {label}</td>
            <td style="padding:3px 8px;font-weight:700;color:#1A6B3C;
                       font-size:12px;text-align:right;">+{sv:.4f}</td>
          </tr>"""

    for feat, sv in top_neg.items():
        label = FEATURE_LABELS.get(feat, feat)
        shap_rows_html += f"""
          <tr style="background:#FEF0EE;">
            <td style="padding:3px 8px;font-size:12px;color:#A62B2B;">▼ {label}</td>
            <td style="padding:3px 8px;font-weight:700;color:#A62B2B;
                       font-size:12px;text-align:right;">{sv:.4f}</td>
          </tr>"""

    if not shap_rows_html:
        shap_rows_html = (
            '<tr><td colspan="2" style="padding:4px 8px;color:#888;font-size:12px;">'
            'No significant SHAP drivers</td></tr>'
        )

    # ── Assemble full popup HTML ──────────────────────────────────
    html = f"""
    <div style="font-family:Arial,sans-serif;min-width:300px;max-width:360px;">

      <!-- Header -->
      <div style="background:linear-gradient(135deg,#1C3557,#2E75B6);
                  padding:10px 14px;border-radius:6px 6px 0 0;">
        <span style="background:{badge_color};color:white;font-weight:700;
                     font-size:11px;padding:2px 7px;border-radius:10px;">
          #{rank}
        </span>
        <span style="color:white;font-weight:700;font-size:14px;
                     margin-left:8px;">AI Recommended Site</span>
        <div style="color:#B8D4F0;font-size:10px;margin-top:4px;
                    font-family:Courier New,monospace;">{h3_id}</div>
      </div>

      <!-- Probability score -->
      <div style="background:#F7F9FC;padding:8px 14px;border-bottom:1px solid #DDE6F0;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <span style="font-size:12px;color:#555;">Hotspot Probability</span>
          <span style="font-size:18px;font-weight:700;color:{bar_color};">
            {prob:.4f}
          </span>
        </div>
        <div style="background:#E0E0E0;border-radius:4px;height:7px;margin-top:5px;">
          <div style="background:{bar_color};width:{bar_pct}%;height:7px;
                      border-radius:4px;"></div>
        </div>
      </div>

      <!-- Raw feature values -->
      <div style="padding:8px 0 0 0;">
        <div style="font-size:11px;font-weight:700;color:#1C3557;
                    padding:2px 14px;letter-spacing:0.5px;">LOCATION FEATURES</div>
        <table style="width:100%;border-collapse:collapse;">
          {feature_rows_html}
        </table>
      </div>

      <!-- SHAP explanation -->
      <div style="padding:8px 0 4px 0;border-top:1px solid #DDE6F0;margin-top:6px;">
        <div style="font-size:11px;font-weight:700;color:#1C3557;
                    padding:2px 14px;letter-spacing:0.5px;">
          SHAP DRIVERS
          <span style="font-weight:400;color:#888;font-size:10px;">
            (why this hex was selected)
          </span>
        </div>
        <table style="width:100%;border-collapse:collapse;margin-top:4px;">
          {shap_rows_html}
        </table>
        <div style="font-size:9px;color:#AAA;padding:4px 14px 6px 14px;">
          Base value: {base_value:.4f}  ·
          Sum of shown drivers: {top_pos.sum() + top_neg.sum():.4f}
        </div>
      </div>

    </div>
    """
    return html


# ─────────────────────────────────────────────────────────────────
#  STEP 6 — Build the Folium map
# ─────────────────────────────────────────────────────────────────
def build_map(
    approved:      pd.DataFrame,
    top_df:        pd.DataFrame,
    training:      pd.DataFrame,
    shap_df:       pd.DataFrame,
    base_value:    float,
    feature_cols:  list,
    top_n:         int,
) -> folium.Map:
    """
    Assemble the three-layer Folium map.

    Layer order (bottom to top):
      1. Background choropleth — all approved hexes (grey → blue)
      2. Ground-truth cafes    — blue circle markers
      3. Top-50 recommended    — polygon fill + SHAP popup
    """
    print(f"\n{'='*60}")
    print("STEP 4 — Building Folium map")
    print(f"{'='*60}")

    # ── Base map ─────────────────────────────────────────────────
    fmap = folium.Map(
        location=[LAHORE_LAT, LAHORE_LON],
        zoom_start=INITIAL_ZOOM,
        tiles=None,       # we add tiles manually to control names
        control_scale=True,
    )

    # Tile options (CartoDB gives clean dark/light options)
    folium.TileLayer(
        "CartoDB positron",
        name="Light Basemap",
        control=True,
    ).add_to(fmap)
    folium.TileLayer(
        "CartoDB dark_matter",
        name="Dark Basemap",
        control=True,
    ).add_to(fmap)
    folium.TileLayer(
        "OpenStreetMap",
        name="OpenStreetMap",
        control=True,
    ).add_to(fmap)

    # ── Colour scale for choropleth + top-50 ─────────────────────
    all_probs = approved["xgb_hotspot_probability"].values
    prob_min  = float(all_probs.min())
    prob_max  = float(all_probs.max())

    # Background choropleth: light grey → blue
    choropleth_cmap = cm.LinearColormap(
        colors=["#D0DCE8", "#7BAFD4", "#2E75B6", "#1C3557"],
        vmin=prob_min,
        vmax=prob_max,
        caption="XGBoost Hotspot Probability (all approved hexes)",
    )
    choropleth_cmap.add_to(fmap)

    # Top-50 colormap: yellow → orange → red (stands out from background)
    top50_cmap = cm.LinearColormap(
        colors=["#FFF176", "#FF9800", "#D32F2F"],
        vmin=top_df["xgb_hotspot_probability"].min(),
        vmax=top_df["xgb_hotspot_probability"].max(),
        caption=f"Top-{top_n} Recommendation Score",
    )

    # ── LAYER 1: Background choropleth (all approved hexes) ───────
    bg_layer = folium.FeatureGroup(name="Hotspot Probability Heatmap (all hexes)", show=True)

    for _, row in approved.iterrows():
        try:
            coords = h3_to_folium_coords(row[INDEX_COL])
        except Exception:
            continue

        prob  = row["xgb_hotspot_probability"]
        color = choropleth_cmap(prob)

        folium.Polygon(
            locations=coords,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.45,
            weight=0.4,
            tooltip=folium.Tooltip(
                f"Score: {prob:.4f}",
                sticky=False,
            ),
        ).add_to(bg_layer)

    bg_layer.add_to(fmap)
    print(f"  Layer 1: {len(approved):,} background choropleth hexes")

    # ── LAYER 2: Ground-truth cafes (275 known locations) ─────────
    cafe_layer = folium.FeatureGroup(
        name="Known Cafes / Restaurants (Ground Truth — 275)", show=True
    )

    cafes = training[training[TARGET_COL] == 1].copy()

    # Get lat/lon from h3_index centroids
    def hex_center_latlon(h3_idx):
        try:
            lat, lon = h3.cell_to_latlng(h3_idx)
            return lat, lon
        except Exception:
            return None, None

    n_cafe_markers = 0
    for _, row in cafes.iterrows():
        lat, lon = hex_center_latlon(row[INDEX_COL])
        if lat is None:
            continue

        folium.CircleMarker(
            location=[lat, lon],
            radius=5,
            color="#1565C0",
            fill=True,
            fill_color="#42A5F5",
            fill_opacity=0.8,
            weight=1.2,
            tooltip=folium.Tooltip(
                f"<b>Known Cafe Location</b><br>"
                f"H3: {row[INDEX_COL][:12]}…",
                sticky=False,
            ),
        ).add_to(cafe_layer)
        n_cafe_markers += 1

    cafe_layer.add_to(fmap)
    print(f"  Layer 2: {n_cafe_markers:,} ground-truth cafe markers")

    # ── LAYER 3: Top-50 AI recommendations (SHAP popup) ──────────
    top50_layer = folium.FeatureGroup(
        name=f"AI Recommendations — Top {top_n} Sites", show=True
    )

    n_rec_markers = 0
    for i, (_, row) in enumerate(top_df.iterrows()):
        try:
            coords   = h3_to_folium_coords(row[INDEX_COL])
            centroid = h3_centroid(row[INDEX_COL])
        except Exception:
            continue

        shap_row = shap_df.iloc[i]
        prob     = row["xgb_hotspot_probability"]
        rank     = int(row["map_rank"])
        color    = top50_cmap(prob)

        # ── Hex polygon fill ──────────────────────────────────────
        popup_html = build_shap_popup(row, shap_row, feature_cols, base_value)
        popup = folium.Popup(
            folium.IFrame(html=popup_html, width=370, height=420),
            max_width=380,
        )

        folium.Polygon(
            locations=coords,
            color="#8B0000",
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            weight=1.8,
            popup=popup,
            tooltip=folium.Tooltip(
                f"<b>Rank #{rank}</b> &nbsp;|&nbsp; Score: {prob:.4f}<br>"
                f"<i>Click for SHAP breakdown</i>",
                sticky=False,
            ),
        ).add_to(top50_layer)

        # ── Rank number label (DivIcon, no image dependency) ──────
        badge_bg = "#B8860B" if rank <= 10 else "#C0392B"
        folium.Marker(
            location=centroid,
            icon=folium.DivIcon(
                html=f"""
                  <div style="
                    background:{badge_bg};
                    color:white;
                    font-weight:700;
                    font-size:10px;
                    width:22px;height:22px;
                    line-height:22px;
                    text-align:center;
                    border-radius:50%;
                    border:2px solid white;
                    box-shadow:0 1px 4px rgba(0,0,0,0.4);
                  ">{rank}</div>""",
                icon_size=(22, 22),
                icon_anchor=(11, 11),
            ),
            tooltip=f"Site #{rank}",
        ).add_to(top50_layer)

        n_rec_markers += 1

    top50_layer.add_to(fmap)
    print(f"  Layer 3: {n_rec_markers:,} recommendation polygons with SHAP popups")

    # ── Map controls ──────────────────────────────────────────────
    folium.LayerControl(collapsed=False).add_to(fmap)
    MiniMap(toggle_display=True, tile_layer="CartoDB positron").add_to(fmap)

    # ── Title & legend HTML overlay ───────────────────────────────
    title_html = f"""
    <div style="
      position:fixed;
      top:10px;left:60px;
      z-index:9999;
      background:rgba(28,53,87,0.93);
      color:white;
      padding:10px 16px;
      border-radius:8px;
      font-family:Arial,sans-serif;
      box-shadow:0 3px 10px rgba(0,0,0,0.35);
      max-width:320px;
    ">
      <div style="font-size:14px;font-weight:700;margin-bottom:4px;">
        Geospatial AI — Cafe Site Selection
      </div>
      <div style="font-size:11px;color:#B8D4F0;margin-bottom:8px;">
        Lahore, Pakistan · XGBoost PU Spy-Bagging · SHAP Explainability
      </div>
      <div style="display:flex;gap:14px;font-size:11px;">
        <span>
          <span style="display:inline-block;width:12px;height:12px;
                       background:#42A5F5;border-radius:50%;margin-right:4px;"></span>
          Known Cafes (275)
        </span>
        <span>
          <span style="display:inline-block;width:12px;height:12px;
                       background:#FF9800;border-radius:2px;margin-right:4px;"></span>
          AI Top {top_n}
        </span>
      </div>
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(title_html))

    # ── Click-me hint (auto-hides after 5 seconds) ────────────────
    hint_html = """
    <div id="map-hint" style="
      position:fixed;
      bottom:60px;left:50%;transform:translateX(-50%);
      z-index:9999;
      background:rgba(46,117,182,0.92);
      color:white;font-family:Arial,sans-serif;font-size:12px;
      padding:8px 18px;border-radius:20px;
      box-shadow:0 2px 8px rgba(0,0,0,0.3);
      pointer-events:none;
    ">
      Click on a numbered gold/red hexagon to see the SHAP explanation
    </div>
    <script>
      setTimeout(function(){
        var el = document.getElementById('map-hint');
        if(el){ el.style.transition='opacity 1s'; el.style.opacity='0';
                setTimeout(function(){ el.remove(); }, 1000); }
      }, 5000);
    </script>
    """
    fmap.get_root().html.add_child(folium.Element(hint_html))

    return fmap


# ─────────────────────────────────────────────────────────────────
#  STEP 7 — Save map and print summary
# ─────────────────────────────────────────────────────────────────
def save_map(fmap: folium.Map, output_path: str):
    print(f"\n{'='*60}")
    print("STEP 5 — Saving HTML map")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fmap.save(output_path)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"  Saved → {output_path}  ({size_kb:.0f} KB)")
    print(f"  Open in any browser — no server required.")


# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    print(f"\n{'='*60}")
    print("generate_final_map.py — SHAP + Folium Interactive Map")
    print(f"  Top-N      : {args.top_n}")
    print(f"  Output     : {args.output}")
    print(f"{'='*60}")

    os.makedirs("outputs", exist_ok=True)

    # 1. Load all inputs
    approved, training, model = load_inputs(args)

    # 2. Select top-N and merge features
    top_df, feature_cols = prepare_top_hexes(approved, training, args.top_n)

    # 3. SHAP values for the top-N hexes
    shap_df, base_value = compute_shap_values(model, top_df, feature_cols)

    # 4. Build the Folium map
    fmap = build_map(
        approved     = approved,
        top_df       = top_df,
        training     = training,
        shap_df      = shap_df,
        base_value   = base_value,
        feature_cols = feature_cols,
        top_n        = args.top_n,
    )

    # 5. Save
    save_map(fmap, args.output)

    # ── Export SHAP table as CSV for the report ───────────────────
    shap_export = top_df[[INDEX_COL, "xgb_hotspot_probability", "map_rank"]].copy()
    for col in feature_cols:
        shap_export[f"shap_{col}"] = shap_df[col].values
    shap_csv_path = os.path.join("outputs", "shap_values_top50.csv")
    shap_export.to_csv(shap_csv_path, index=False)
    print(f"  SHAP values exported → {shap_csv_path}")

    # ── Final summary ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Map          : {args.output}")
    print(f"  SHAP table   : {shap_csv_path}")
    print(f"\n  Top 5 AI-recommended cafe sites in Lahore:")
    print(f"  {'Rank':<6} {'H3 Index':<18} {'Score':>8}  Top SHAP Driver")
    print(f"  {'─'*60}")
    for _, row in top_df.head(5).iterrows():
        i       = row["map_rank"] - 1
        sv      = shap_df.iloc[i]
        top_feat = sv.abs().idxmax()
        top_label = FEATURE_LABELS.get(top_feat, top_feat)
        print(
            f"  #{int(row['map_rank']):<5} {row[INDEX_COL]:<18} "
            f"{row['xgb_hotspot_probability']:>8.4f}  {top_label}"
        )
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
