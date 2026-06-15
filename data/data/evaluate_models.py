"""
evaluate_models.py
==================
Phase 1 of the Final Evaluation Pipeline.

Trains two models on training_data.csv and produces a side-by-side
Precision-Recall AUC comparison plot, saved to outputs/plots/.

Models
------
  BASELINE : Logistic Regression
             Treats all 3,416 unlabelled zeros as true negatives.
             Provides the lower-bound PR-AUC benchmark.

  PROPOSED : XGBoost — Spy + Weighted Bagging (PU Learning)
             Identifies Reliable Negatives via a spy step, then runs
             N_BAGS balanced bagging iterations. The average of all
             per-bag probabilities is the final hotspot_probability.

Why PR-AUC (not ROC-AUC)?
--------------------------
With 275 positives vs 3,416 unlabelled (ratio ~1:12.4), the ROC curve
is overly optimistic because it rewards true-negative recall — easy
when negatives dominate. The Precision-Recall curve is honest: it only
rewards performance on the minority positive class, which is exactly
what we care about.

Install
-------
  pip install pandas numpy scikit-learn xgboost matplotlib joblib

Usage
-----
  python evaluate_models.py
  python evaluate_models.py --data path/to/training_data.csv
  python evaluate_models.py --bags 200 --spy-ratio 0.15 --seed 143
"""

import os
import argparse
import time
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")          # headless — no display required
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import joblib

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    precision_recall_curve,
    average_precision_score,
    roc_auc_score,
    f1_score,
    classification_report,
)
import xgboost as xgb

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
#  CONFIGURATION  (override via CLI args below)
# ─────────────────────────────────────────────────────────────────
DEFAULT_DATA_PATH  = "training_data.csv"
OUTPUT_DIR         = "outputs"
PLOT_DIR           = os.path.join(OUTPUT_DIR, "plots")
MODEL_DIR          = os.path.join(OUTPUT_DIR, "models")
METRICS_PATH       = os.path.join(OUTPUT_DIR, "model_metrics.csv")

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
TARGET_COL = "is_hotspot"
INDEX_COL  = "h3_index"

# PU Bagging defaults
DEFAULT_N_BAGS    = 100   # number of bagging iterations
DEFAULT_SPY_RATIO = 0.15  # fraction of positives used as spies
DEFAULT_SEED      = 143   # matches student ID

# Palette
C_BASELINE = "#E05C2E"   # warm orange for Logistic Regression
C_PROPOSED = "#1C6EAF"   # navy blue  for XGBoost PU
C_FILL_B   = "#FDDDC9"
C_FILL_P   = "#C5DCEF"
C_GRID     = "#E8EEF4"


# ─────────────────────────────────────────────────────────────────
#  ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Evaluate baseline vs PU XGBoost model.")
    p.add_argument("--data",      default=DEFAULT_DATA_PATH, help="Path to training_data.csv")
    p.add_argument("--bags",      type=int,   default=DEFAULT_N_BAGS,    help="Number of bagging iterations")
    p.add_argument("--spy-ratio", type=float, default=DEFAULT_SPY_RATIO, help="Spy fraction (0–1)")
    p.add_argument("--seed",      type=int,   default=DEFAULT_SEED,      help="Random seed")
    p.add_argument("--cv-folds",  type=int,   default=5,                 help="CV folds for stable PR-AUC")
    p.add_argument("--no-save",   action="store_true", help="Skip saving models to disk")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────
#  DATA LOADING & VALIDATION
# ─────────────────────────────────────────────────────────────────
def load_data(path: str):
    print(f"\n{'='*60}")
    print("STEP 0 — Loading Data")
    print(f"{'='*60}")

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"training_data.csv not found at '{path}'.\n"
            "Run build_feature_matrix.py first."
        )

    df = pd.read_csv(path)
    print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns from '{path}'")

    # ── Validate expected columns ─────────────────────────────────
    missing = [c for c in FEATURE_COLS + [TARGET_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}\nFound: {list(df.columns)}")

    # ── Fill any residual NaNs with column median (safety net) ────
    nan_counts = df[FEATURE_COLS].isna().sum()
    if nan_counts.any():
        print(f"  [WARN] NaNs detected — filling with column median:")
        for col, count in nan_counts[nan_counts > 0].items():
            med = df[col].median()
            df[col] = df[col].fillna(med)
            print(f"         {col}: {count} NaNs → median ({med:.2f})")

    X = df[FEATURE_COLS].values.astype(np.float32)
    y = df[TARGET_COL].values.astype(int)
    h3_ids = df[INDEX_COL].values if INDEX_COL in df.columns else None

    n_pos = int(y.sum())
    n_neg = int((y == 0).sum())
    ratio = n_neg / max(n_pos, 1)

    print(f"\n  Positive (is_hotspot=1): {n_pos:,}  |  Unlabelled (is_hotspot=0): {n_neg:,}")
    print(f"  Class ratio  neg:pos = {ratio:.1f}:1")
    print(f"  Features: {FEATURE_COLS}")

    return X, y, h3_ids, df


# ─────────────────────────────────────────────────────────────────
#  BASELINE — Logistic Regression
# ─────────────────────────────────────────────────────────────────
def train_logistic_regression(X: np.ndarray, y: np.ndarray, seed: int, cv_folds: int):
    """
    Train a standard Logistic Regression (class_weight='balanced').

    We use StratifiedKFold cross-validation to produce stable PR-AUC
    estimates, then return the fold-averaged precision/recall curve
    and the model fitted on the full dataset.

    class_weight='balanced' is the only concession to imbalance —
    it rescales per-class loss but does NOT address the PU learning
    problem (unlabelled zeros are still treated as true negatives).
    """
    print(f"\n{'='*60}")
    print("STEP 1 — Baseline: Logistic Regression")
    print(f"{'='*60}")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    skf = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=seed)

    fold_precisions = []
    fold_recalls    = []
    fold_ap_scores  = []

    t0 = time.time()

    for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X_scaled, y)):
        X_tr, X_val = X_scaled[train_idx], X_scaled[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        model = LogisticRegression(
            class_weight="balanced",
            max_iter=2000,
            solver="lbfgs",
            C=1.0,
            random_state=seed,
        )
        model.fit(X_tr, y_tr)

        y_prob = model.predict_proba(X_val)[:, 1]
        ap = average_precision_score(y_val, y_prob)
        fold_ap_scores.append(ap)

        # Interpolate to 300 recall points for consistent averaging
        prec, rec, _ = precision_recall_curve(y_val, y_prob)
        rec_interp  = np.linspace(0, 1, 300)
        prec_interp = np.interp(rec_interp, rec[::-1], prec[::-1])
        fold_precisions.append(prec_interp)
        fold_recalls.append(rec_interp)

        print(f"  Fold {fold_idx+1}/{cv_folds}  AP={ap:.4f}")

    mean_ap   = float(np.mean(fold_ap_scores))
    std_ap    = float(np.std(fold_ap_scores))
    mean_prec = np.mean(fold_precisions, axis=0)
    mean_rec  = np.mean(fold_recalls,    axis=0)

    elapsed = time.time() - t0
    print(f"\n  ► Baseline PR-AUC : {mean_ap:.4f} ± {std_ap:.4f}  ({elapsed:.1f}s)")

    # Refit on full dataset for saving
    full_model = LogisticRegression(
        class_weight="balanced", max_iter=2000, solver="lbfgs", C=1.0, random_state=seed
    )
    full_model.fit(X_scaled, y)
    full_probs = full_model.predict_proba(X_scaled)[:, 1]

    return {
        "model":     full_model,
        "scaler":    scaler,
        "probs":     full_probs,
        "mean_prec": mean_prec,
        "mean_rec":  mean_rec,
        "mean_ap":   mean_ap,
        "std_ap":    std_ap,
        "fold_aps":  fold_ap_scores,
    }


# ─────────────────────────────────────────────────────────────────
#  PROPOSED — XGBoost PU Spy + Weighted Bagging
# ─────────────────────────────────────────────────────────────────
def train_pu_xgboost(
    X: np.ndarray,
    y: np.ndarray,
    n_bags: int,
    spy_ratio: float,
    seed: int,
    cv_folds: int,
):
    """
    PU Learning: Spy identification + Weighted Bagging.

    ── SPY STEP ────────────────────────────────────────────────────
    A fraction (spy_ratio) of the positive examples are secretly
    relabelled as 0 and mixed into the unlabelled pool.  A quick
    first-pass XGBoost is trained on this contaminated dataset.

    The minimum predict_proba score assigned to any spy becomes the
    confidence threshold (t).  Any unlabelled hex scoring below (t)
    is a Reliable Negative — it cannot be a hidden positive because
    even the positives we deliberately hid scored higher.

    ── BAGGING STEP ────────────────────────────────────────────────
    For each of N bags:
      1. Sample len(positives) Reliable Negatives WITH replacement.
      2. Combine with all positives → balanced mini-dataset.
      3. Train one XGBoost estimator.
      4. Predict probability for ALL 3,691 hexes.

    The final hotspot_probability per hex is the mean of N bag scores.
    This ensemble smooths out variance from the random negative sampling.

    ── EVALUATION ──────────────────────────────────────────────────
    We use the same StratifiedKFold procedure as the baseline.
    Within each fold, we run the full spy+bagging procedure so the
    CV estimate reflects real generalisation, not in-bag performance.
    """
    print(f"\n{'='*60}")
    print(f"STEP 2 — Proposed: XGBoost PU Spy + Bagging  ({n_bags} bags)")
    print(f"{'='*60}")

    rng = np.random.default_rng(seed)
    pos_idx = np.where(y == 1)[0]
    neg_idx = np.where(y == 0)[0]
    n_pos   = len(pos_idx)
    n_neg   = len(neg_idx)

    print(f"  Positives: {n_pos}  |  Unlabelled: {n_neg}  |  Spy ratio: {spy_ratio:.0%}")

    # XGBoost base parameters
    # scale_pos_weight = n_neg / n_pos gives proper gradient scaling,
    # but we override it per-bag since bags are balanced.
    xgb_params = dict(
        n_estimators      = 200,
        max_depth         = 5,
        learning_rate     = 0.05,
        subsample         = 0.8,
        colsample_bytree  = 0.8,
        min_child_weight  = 3,
        gamma             = 0.1,
        reg_alpha         = 0.1,
        reg_lambda        = 1.0,
        use_label_encoder = False,
        eval_metric       = "aucpr",
        random_state      = seed,
        n_jobs            = -1,
        verbosity         = 0,
    )

    def run_spy_bagging(X_train, y_train, X_score, n_bags, rng_local):
        """
        Full spy+bagging on a given train split.
        Returns per-sample probabilities for X_score.
        """
        pos_i = np.where(y_train == 1)[0]
        neg_i = np.where(y_train == 0)[0]

        # ── STEP A: Spy identification ────────────────────────────
        n_spies  = max(1, int(len(pos_i) * spy_ratio))
        spy_mask = rng_local.choice(len(pos_i), size=n_spies, replace=False)
        spy_i    = pos_i[spy_mask]

        # Build contaminated labels: spies → 0
        y_contaminated = y_train.copy()
        y_contaminated[spy_i] = 0

        # Quick first-pass XGBoost on contaminated set
        spy_model = xgb.XGBClassifier(
            **{**xgb_params, "n_estimators": 100}
        )
        spy_model.fit(X_train, y_contaminated)

        spy_scores = spy_model.predict_proba(X_train[spy_i])[:, 1]
        threshold  = float(np.min(spy_scores))   # lowest score any spy received

        # Reliable Negatives: unlabelled hexes that scored below the threshold
        neg_scores = spy_model.predict_proba(X_train[neg_i])[:, 1]
        reliable_neg_mask = neg_scores < threshold
        reliable_neg_i    = neg_i[reliable_neg_mask]

        n_reliable = reliable_neg_i.sum() if hasattr(reliable_neg_i, "sum") else len(reliable_neg_i)
        if n_reliable < 5:
            # Fallback: use all unlabelled if threshold is too aggressive
            reliable_neg_i = neg_i

        # ── STEP B: Weighted Bagging ──────────────────────────────
        bag_probs = np.zeros((len(X_score), n_bags), dtype=np.float32)

        for b in range(n_bags):
            # Sample same number of reliable negatives as positives
            bag_neg = rng_local.choice(
                reliable_neg_i, size=len(pos_i), replace=True
            )
            idx_bag = np.concatenate([pos_i, bag_neg])

            X_bag = X_train[idx_bag]
            y_bag = y_train[idx_bag]
            # y_bag for negatives is 0 (reliable), positives are 1

            bag_model = xgb.XGBClassifier(**xgb_params)
            bag_model.fit(X_bag, y_bag)

            bag_probs[:, b] = bag_model.predict_proba(X_score)[:, 1]

        return bag_probs.mean(axis=1), threshold, len(reliable_neg_i)

    # ── Cross-validation for PR-AUC estimate ─────────────────────
    skf = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=seed)
    fold_precisions = []
    fold_recalls    = []
    fold_ap_scores  = []

    t0 = time.time()
    rng_cv = np.random.default_rng(seed)

    for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X, y)):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        # Run reduced bags inside CV (speeds up CV; full bags on final fit)
        cv_bags    = max(20, n_bags // 5)
        fold_probs, thr, n_rel = run_spy_bagging(X_tr, y_tr, X_val, cv_bags, rng_cv)

        ap = average_precision_score(y_val, fold_probs)
        fold_ap_scores.append(ap)

        prec, rec, _ = precision_recall_curve(y_val, fold_probs)
        rec_interp   = np.linspace(0, 1, 300)
        prec_interp  = np.interp(rec_interp, rec[::-1], prec[::-1])
        fold_precisions.append(prec_interp)
        fold_recalls.append(rec_interp)

        print(f"  Fold {fold_idx+1}/{cv_folds}  AP={ap:.4f}  threshold={thr:.4f}  reliable_negs={n_rel}")

    mean_ap   = float(np.mean(fold_ap_scores))
    std_ap    = float(np.std(fold_ap_scores))
    mean_prec = np.mean(fold_precisions, axis=0)
    mean_rec  = np.mean(fold_recalls,    axis=0)

    elapsed = time.time() - t0
    print(f"\n  ► XGBoost PU PR-AUC : {mean_ap:.4f} ± {std_ap:.4f}  ({elapsed:.1f}s)")

    # ── Full-dataset fit (all N bags, for predictions + SHAP) ─────
    print(f"\n  Fitting final model on full dataset ({n_bags} bags) …")
    t1 = time.time()
    final_probs, final_thr, final_n_rel = run_spy_bagging(X, y, X, n_bags, rng)
    print(f"  Done. spy_threshold={final_thr:.4f}  reliable_negs={final_n_rel}  ({time.time()-t1:.1f}s)")

    # ── Refit a single deterministic XGBoost on the best split ────
    # This single model is saved for SHAP analysis in Phase 3.
    n_spies   = max(1, int(n_pos * spy_ratio))
    spy_pool  = rng.choice(n_pos, size=n_spies, replace=False)
    spy_global = pos_idx[spy_pool]
    y_for_spy  = y.copy()
    y_for_spy[spy_global] = 0

    spy_m = xgb.XGBClassifier(**{**xgb_params, "n_estimators": 100})
    spy_m.fit(X, y_for_spy)
    neg_sc = spy_m.predict_proba(X[neg_idx])[:, 1]
    thr_g  = float(np.min(spy_m.predict_proba(X[spy_global])[:, 1]))
    rel_neg = neg_idx[neg_sc < thr_g] if (neg_sc < thr_g).sum() > 5 else neg_idx

    bag_neg_final = rng.choice(rel_neg, size=n_pos, replace=True)
    idx_final = np.concatenate([pos_idx, bag_neg_final])
    single_xgb = xgb.XGBClassifier(**xgb_params)
    single_xgb.fit(X[idx_final], y[idx_final])

    return {
        "model":        single_xgb,   # single model for SHAP
        "probs":        final_probs,   # ensemble average probabilities
        "mean_prec":    mean_prec,
        "mean_rec":     mean_rec,
        "mean_ap":      mean_ap,
        "std_ap":       std_ap,
        "fold_aps":     fold_ap_scores,
        "spy_threshold": final_thr,
        "n_reliable_neg": final_n_rel,
    }


# ─────────────────────────────────────────────────────────────────
#  METRICS TABLE
# ─────────────────────────────────────────────────────────────────
def compute_extra_metrics(y_true, probs, label):
    """Compute ROC-AUC and best-threshold F1 for the metrics CSV."""
    roc  = roc_auc_score(y_true, probs)
    best_f1, best_thr = 0.0, 0.5
    for thr in np.linspace(0.1, 0.9, 81):
        preds = (probs >= thr).astype(int)
        if preds.sum() == 0:
            continue
        f = f1_score(y_true, preds, zero_division=0)
        if f > best_f1:
            best_f1, best_thr = f, thr
    return {"model": label, "roc_auc": round(roc, 4),
            "pr_auc": None,   # filled by caller
            "best_f1": round(best_f1, 4), "best_f1_threshold": round(best_thr, 3)}


# ─────────────────────────────────────────────────────────────────
#  PLOTTING
# ─────────────────────────────────────────────────────────────────
def plot_pr_comparison(lr_res, xgb_res, output_path: str, seed: int):
    """
    Generate a publication-quality PR-AUC comparison figure.

    Layout:
      Left  — Main PR Curve panel (both models + confidence bands)
      Right — Summary stats panel (PR-AUC, ROC-AUC, F1 table)
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fig = plt.figure(figsize=(14, 6.5), facecolor="#F7F9FC")
    fig.suptitle(
        "Model Evaluation: Baseline vs. PU XGBoost — Precision-Recall Curve",
        fontsize=14, fontweight="bold", color="#1A2A3A", y=0.98,
    )

    gs = GridSpec(1, 2, figure=fig, width_ratios=[1.6, 1], wspace=0.06)
    ax_main  = fig.add_subplot(gs[0])
    ax_stats = fig.add_subplot(gs[1])

    # ── Left: PR curves ──────────────────────────────────────────
    ax_main.set_facecolor("#FFFFFF")
    ax_main.grid(color=C_GRID, linewidth=0.7, zorder=0)

    # Baseline — Logistic Regression
    ax_main.fill_between(
        lr_res["mean_rec"], lr_res["mean_prec"],
        alpha=0.18, color=C_BASELINE, zorder=1,
    )
    ax_main.plot(
        lr_res["mean_rec"], lr_res["mean_prec"],
        color=C_BASELINE, linewidth=2.2, linestyle="--", zorder=3,
        label=(
            f"Logistic Regression (Baseline)\n"
            f"PR-AUC = {lr_res['mean_ap']:.4f} ± {lr_res['std_ap']:.4f}"
        ),
    )

    # Proposed — XGBoost PU
    ax_main.fill_between(
        xgb_res["mean_rec"], xgb_res["mean_prec"],
        alpha=0.18, color=C_PROPOSED, zorder=1,
    )
    ax_main.plot(
        xgb_res["mean_rec"], xgb_res["mean_prec"],
        color=C_PROPOSED, linewidth=2.6, linestyle="-", zorder=4,
        label=(
            f"XGBoost PU Spy-Bagging (Proposed)\n"
            f"PR-AUC = {xgb_res['mean_ap']:.4f} ± {xgb_res['std_ap']:.4f}"
        ),
    )

    # No-skill baseline: a random classifier has precision = prevalence
    prevalence = 275 / 3691
    ax_main.axhline(
        y=prevalence, color="#999999", linewidth=1.2, linestyle=":",
        label=f"No-skill classifier (prevalence={prevalence:.3f})",
    )

    ax_main.set_xlabel("Recall", fontsize=12, color="#333333")
    ax_main.set_ylabel("Precision", fontsize=12, color="#333333")
    ax_main.set_xlim(0, 1); ax_main.set_ylim(0, 1.02)
    ax_main.tick_params(labelsize=10, colors="#555555")
    ax_main.legend(
        loc="upper right", fontsize=9.5, framealpha=0.95,
        facecolor="white", edgecolor="#CCCCCC",
    )
    ax_main.set_title(
        f"5-Fold Cross-Validated PR Curves  (seed={seed})",
        fontsize=11, color="#444444", pad=8,
    )

    # Improvement annotation
    delta = xgb_res["mean_ap"] - lr_res["mean_ap"]
    pct   = delta / max(lr_res["mean_ap"], 1e-9) * 100
    ax_main.annotate(
        f"+{delta:.4f} PR-AUC\n({pct:+.1f}% lift)",
        xy=(0.55, xgb_res["mean_prec"][int(0.55 * 300)]),
        xytext=(0.38, 0.72),
        fontsize=9.5, color=C_PROPOSED, fontweight="bold",
        arrowprops=dict(arrowstyle="->", color=C_PROPOSED, lw=1.3),
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                  edgecolor=C_PROPOSED, alpha=0.9),
    )

    # ── Right: summary table ─────────────────────────────────────
    ax_stats.set_facecolor("#FFFFFF")
    ax_stats.axis("off")

    rows = [
        ["Metric", "LR Baseline", "XGBoost PU", "Δ"],
        ["PR-AUC",
         f"{lr_res['mean_ap']:.4f}",
         f"{xgb_res['mean_ap']:.4f}",
         f"{delta:+.4f}"],
        ["PR-AUC std",
         f"±{lr_res['std_ap']:.4f}",
         f"±{xgb_res['std_ap']:.4f}",
         "—"],
    ]

    # Add per-fold breakdown
    for i, (ap_lr, ap_xgb) in enumerate(
        zip(lr_res["fold_aps"], xgb_res["fold_aps"])
    ):
        rows.append([
            f"Fold {i+1}",
            f"{ap_lr:.4f}",
            f"{ap_xgb:.4f}",
            f"{ap_xgb - ap_lr:+.4f}",
        ])

    col_w = [0.28, 0.25, 0.25, 0.22]
    col_x = [0.02, 0.30, 0.56, 0.80]
    row_h = 0.75 / max(len(rows), 1)

    for ri, row in enumerate(rows):
        is_header = ri == 0
        bg_color  = "#1C3557" if is_header else ("#EAF3FB" if ri % 2 == 0 else "white")
        txt_color = "white"   if is_header else "#1A2A3A"

        for ci, (cell, cx, cw) in enumerate(zip(row, col_x, col_w)):
            ax_stats.add_patch(plt.Rectangle(
                (cx, 0.88 - ri * row_h), cw, row_h * 0.92,
                facecolor=bg_color, edgecolor="#CCCCCC",
                transform=ax_stats.transAxes, clip_on=False,
            ))
            # Colour the Δ column green/red
            if ci == 3 and not is_header and cell.startswith("+"):
                txt_color = "#1A6B3C"
            elif ci == 3 and not is_header and cell.startswith("-"):
                txt_color = "#A62B2B"

            ax_stats.text(
                cx + cw / 2, 0.88 - ri * row_h + row_h * 0.45,
                cell,
                ha="center", va="center",
                fontsize=8.2 if not is_header else 8.5,
                color=txt_color if (is_header or ci == 3) else "#1A2A3A",
                fontweight="bold" if is_header else "normal",
                transform=ax_stats.transAxes,
            )

    ax_stats.set_title(
        "Per-Fold PR-AUC Summary", fontsize=11, color="#444444", pad=12,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#F7F9FC")
    plt.close(fig)
    print(f"\n  Plot saved → {output_path}")


# ─────────────────────────────────────────────────────────────────
#  FEATURE IMPORTANCE PLOT (bonus: XGBoost gain-based)
# ─────────────────────────────────────────────────────────────────
def plot_feature_importance(xgb_model, output_dir: str):
    fi_path = os.path.join(output_dir, "feature_importance.png")

    importances = xgb_model.feature_importances_
    order = np.argsort(importances)[::-1]
    sorted_feats = [FEATURE_COLS[i] for i in order]
    sorted_imps  = importances[order]

    fig, ax = plt.subplots(figsize=(9, 4.5), facecolor="#F7F9FC")
    ax.set_facecolor("#FFFFFF")
    ax.grid(axis="x", color=C_GRID, linewidth=0.7, zorder=0)

    bars = ax.barh(
        sorted_feats[::-1], sorted_imps[::-1],
        color=[C_PROPOSED if i < 3 else "#7BAFD4" for i in range(len(sorted_feats) - 1, -1, -1)],
        edgecolor="white", height=0.62, zorder=3,
    )

    for bar, val in zip(bars, sorted_imps[::-1]):
        ax.text(
            bar.get_width() + 0.002, bar.get_y() + bar.get_height() / 2,
            f"{val:.4f}", va="center", fontsize=9, color="#333333",
        )

    ax.set_xlabel("XGBoost Feature Importance (gain)", fontsize=11, color="#333333")
    ax.set_title("Feature Importance — XGBoost PU Model", fontsize=12,
                 color="#1A2A3A", fontweight="bold")
    ax.tick_params(labelsize=9.5)
    plt.tight_layout()
    fig.savefig(fi_path, dpi=150, bbox_inches="tight", facecolor="#F7F9FC")
    plt.close(fig)
    print(f"  Feature importance plot → {fi_path}")


# ─────────────────────────────────────────────────────────────────
#  SAVE ARTEFACTS
# ─────────────────────────────────────────────────────────────────
def save_artefacts(
    lr_res, xgb_res, df, h3_ids, X, y, args
):
    os.makedirs(MODEL_DIR, exist_ok=True)

    # ── Predictions CSV ───────────────────────────────────────────
    pred_df = pd.DataFrame({
        "h3_index":                  h3_ids if h3_ids is not None else np.arange(len(y)),
        "is_hotspot":                y,
        "lr_hotspot_probability":    lr_res["probs"],
        "xgb_hotspot_probability":   xgb_res["probs"],
    })
    pred_df["xgb_rank"] = pred_df["xgb_hotspot_probability"].rank(
        ascending=False, method="first"
    ).astype(int)

    pred_path = os.path.join(OUTPUT_DIR, "predictions.csv")
    pred_df.to_csv(pred_path, index=False)
    print(f"\n  Predictions saved → {pred_path}")
    print(f"  Top-5 recommended hexes:")
    top5 = pred_df.nsmallest(5, "xgb_rank")[
        ["h3_index", "xgb_hotspot_probability", "xgb_rank"]
    ]
    print(top5.to_string(index=False))

    # ── Model metrics CSV ─────────────────────────────────────────
    metrics = pd.DataFrame([
        {
            "model": "Logistic Regression (Baseline)",
            "pr_auc_mean": lr_res["mean_ap"],
            "pr_auc_std":  lr_res["std_ap"],
            "cv_folds":    len(lr_res["fold_aps"]),
        },
        {
            "model": "XGBoost PU Spy-Bagging (Proposed)",
            "pr_auc_mean": xgb_res["mean_ap"],
            "pr_auc_std":  xgb_res["std_ap"],
            "cv_folds":    len(xgb_res["fold_aps"]),
        },
    ])
    metrics.to_csv(METRICS_PATH, index=False)
    print(f"  Metrics CSV → {METRICS_PATH}")

    # ── Serialise models ──────────────────────────────────────────
    if not args.no_save:
        joblib.dump(lr_res["model"],  os.path.join(MODEL_DIR, "lr_baseline.pkl"))
        joblib.dump(lr_res["scaler"], os.path.join(MODEL_DIR, "lr_scaler.pkl"))
        xgb_res["model"].save_model(  os.path.join(MODEL_DIR, "xgb_pu_model.json"))
        print(f"  Models saved → {MODEL_DIR}/")

    return pred_df


# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    np.random.seed(args.seed)

    # ── Setup output directories ──────────────────────────────────
    for d in [OUTPUT_DIR, PLOT_DIR, MODEL_DIR]:
        os.makedirs(d, exist_ok=True)

    # ── Load data ─────────────────────────────────────────────────
    X, y, h3_ids, df = load_data(args.data)

    # ── Train baseline ────────────────────────────────────────────
    lr_res = train_logistic_regression(X, y, args.seed, args.cv_folds)

    # ── Train proposed PU model ───────────────────────────────────
    xgb_res = train_pu_xgboost(
        X, y,
        n_bags    = args.bags,
        spy_ratio = args.spy_ratio,
        seed      = args.seed,
        cv_folds  = args.cv_folds,
    )

    # ── Plot PR-AUC comparison ────────────────────────────────────
    print(f"\n{'='*60}")
    print("STEP 3 — Generating PR-AUC Comparison Plot")
    print(f"{'='*60}")
    plot_path = os.path.join(PLOT_DIR, "pr_auc_comparison.png")
    plot_pr_comparison(lr_res, xgb_res, plot_path, args.seed)
    plot_feature_importance(xgb_res["model"], PLOT_DIR)

    # ── Save all artefacts ────────────────────────────────────────
    print(f"\n{'='*60}")
    print("STEP 4 — Saving Artefacts")
    print(f"{'='*60}")
    pred_df = save_artefacts(lr_res, xgb_res, df, h3_ids, X, y, args)

    # ── Final summary ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("EVALUATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Baseline  PR-AUC : {lr_res['mean_ap']:.4f} ± {lr_res['std_ap']:.4f}")
    print(f"  Proposed  PR-AUC : {xgb_res['mean_ap']:.4f} ± {xgb_res['std_ap']:.4f}")
    delta = xgb_res["mean_ap"] - lr_res["mean_ap"]
    pct   = delta / max(lr_res["mean_ap"], 1e-9) * 100
    print(f"  Lift             : {delta:+.4f}  ({pct:+.1f}%)")
    print(f"\n  Key output files:")
    print(f"    {os.path.join(PLOT_DIR, 'pr_auc_comparison.png')}")
    print(f"    {os.path.join(PLOT_DIR, 'feature_importance.png')}")
    print(f"    {os.path.join(OUTPUT_DIR, 'predictions.csv')}")
    print(f"    {METRICS_PATH}")
    if not args.no_save:
        print(f"    {MODEL_DIR}/xgb_pu_model.json  ← used by generate_final_map.py")
        print(f"    {MODEL_DIR}/lr_baseline.pkl")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
