"""Separate training, persistence, inference and SHAP for the three penalties."""
import time

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import GridSearchCV, GroupShuffleSplit, StratifiedGroupKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from rtap.events import FEATURES


def make_pipeline(penalty, C=1., l1_ratio=0.5, max_iter=1000):
    options = dict(penalty=penalty, C=C, solver="saga", max_iter=max_iter, random_state=42)
    if penalty == "elasticnet":
        options["l1_ratio"] = l1_ratio
    return Pipeline([("impute", SimpleImputer(strategy="median", keep_empty_features=True)),
                     ("scale", StandardScaler()), ("model", LogisticRegression(**options))])


def illustrative_dataset():
    """Independent scene labels are latent occupancy, never model predictions."""
    rng = np.random.default_rng(42)
    rows = []
    for session in range(60):
        persons = int(rng.integers(0, 7))
        lighting_error = rng.normal(0, .3)
        for observation in range(8):
            measured = max(0., persons + lighting_error + rng.normal(0, .65))
            noise = float(rng.uniform(0, .6))
            rows.append(dict(row_id=f"synthetic-{session:03d}-{observation}", session_id=session,
                             avg_person=measured, avg_noise=noise,
                             avg_voice=float(rng.uniform(0, 1)), avg_speech=float(rng.integers(0, 2)),
                             avg_objects=measured+float(rng.uniform(0, 4)), label=int(persons >= 3),
                             true_persons=persons, provenance="ILLUSTRATIVE_SYNTHETIC"))
    return pd.DataFrame(rows)


def train_models(data):
    X, y, groups = data[FEATURES], data["label"].astype(int), data["session_id"]
    train, test = next(GroupShuffleSplit(n_splits=1, test_size=.25, random_state=42).split(X, y, groups))
    if len(set(y.iloc[train])) != 2 or len(set(y.iloc[test])) != 2:
        raise ValueError("Both train and test must contain both classes")
    assert not set(groups.iloc[train]) & set(groups.iloc[test])
    cv = StratifiedGroupKFold(n_splits=3, shuffle=True, random_state=42)
    models, metrics, searches = {}, [], []
    for name, penalty in (("L1", "l1"), ("L2", "l2"), ("ENet", "elasticnet")):
        baseline = make_pipeline(penalty)
        baseline_cv = cross_val_score(baseline, X.iloc[train], y.iloc[train],
                                      groups=groups.iloc[train], cv=cv, scoring="f1")
        grid = {"model__C": [.01, .1, 1, 10], "model__max_iter": [1000, 3000]}
        if penalty == "elasticnet":
            grid["model__l1_ratio"] = [.2, .5, .8]
        start = time.perf_counter()
        search = GridSearchCV(make_pipeline(penalty), grid, scoring="f1", cv=cv,
                              n_jobs=1, error_score="raise", return_train_score=True)
        search.fit(X.iloc[train], y.iloc[train], groups=groups.iloc[train])
        searches.append(pd.DataFrame(search.cv_results_).assign(family=name))
        elapsed = time.perf_counter()-start
        for label, pipeline, score in ((name, baseline.fit(X.iloc[train], y.iloc[train]), float(baseline_cv.mean())),
                                      (name+" tuned", search.best_estimator_, float(search.best_score_))):
            predicted = pipeline.predict(X.iloc[test])
            models[label] = pipeline
            metrics.append(dict(model=label, cv_f1=score, test_f1=float(f1_score(y.iloc[test], predicted)),
                                test_precision=float(precision_score(y.iloc[test], predicted, zero_division=0)),
                                test_recall=float(recall_score(y.iloc[test], predicted, zero_division=0)),
                                confusion_matrix=confusion_matrix(y.iloc[test], predicted, labels=[0,1]).tolist(),
                                coefficients=pipeline.named_steps["model"].coef_[0].tolist(),
                                params={k: pipeline.named_steps["model"].get_params()[k]
                                        for k in ("C", "penalty", "l1_ratio", "max_iter")},
                                search_seconds=elapsed if "tuned" in label else None))
    predictions = data.iloc[test][["row_id", "session_id", "label"]].copy()
    for label, model in models.items():
        predictions[label] = model.predict(X.iloc[test])
    bundle = dict(models=models, background=X.iloc[train].sample(min(100,len(train)), random_state=42),
                  features=FEATURES, provenance="ILLUSTRATIVE_SYNTHETIC", created=time.time(),
                  train_ids=data.iloc[train].row_id.tolist(), test_ids=data.iloc[test].row_id.tolist(),
                  train_sessions=sorted(set(groups.iloc[train])), test_sessions=sorted(set(groups.iloc[test])))
    return bundle, metrics, predictions, pd.concat(searches, ignore_index=True)


def build_explainers(bundle):
    import shap
    return {name: shap.LinearExplainer(model.named_steps["model"], model[:-1].transform(bundle["background"]))
            for name, model in bundle["models"].items()}


def infer(bundle, row, model_name="ENet tuned"):
    frame = pd.DataFrame([row])[FEATURES]
    model = bundle["models"][model_name]
    probability = float(model.predict_proba(frame)[0,1])
    return dict(label="grup" if probability >= .5 else "solo", probability=probability,
                missing_features=[c for c in FEATURES if pd.isna(frame.iloc[0][c])])


def explain(bundle, explainers, row, model_name="ENet tuned"):
    model = bundle["models"][model_name]
    transformed = model[:-1].transform(pd.DataFrame([row])[FEATURES])
    values = explainers[model_name](transformed)
    reconstructed = float(np.asarray(values.base_values).ravel()[0] + values.values[0].sum())
    decision = float(model.named_steps["model"].decision_function(transformed)[0])
    if not np.isclose(reconstructed, decision, atol=1e-7):
        raise AssertionError("SHAP additivity failed")
    return pd.DataFrame({"Feature": FEATURES, "SHAP log-odds": values.values[0]}), reconstructed
