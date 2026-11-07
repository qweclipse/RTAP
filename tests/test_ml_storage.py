from pathlib import Path
import joblib
import numpy as np
import pandas as pd
import pytest
from rtap.config import ROOT
from rtap.events import FEATURES
from rtap.ml import make_pipeline, build_explainers, explain, infer
from rtap.storage import read_aggregates, write_csv


def test_atomic_csv_idempotence_and_incomplete_files(tmp_path):
    row = dict(window_start="2026-10-23T00:00:00Z",window_end="2026-10-23T00:00:10Z",source_id="TEST",avg_person=3)
    write_csv(tmp_path/"batch-000000000001.csv",[row])
    write_csv(tmp_path/"batch-000000000001.csv",[row])
    (tmp_path/"batch-000000000002.csv.tmp").write_text("partial")
    assert len(read_aggregates(tmp_path)) == 1


def test_preprocessor_fits_training_only():
    x = pd.DataFrame([[0,.1,.2,0,1],[1,.2,.3,0,2],[4,.3,.5,1,5],[5,.4,.6,1,6]],columns=FEATURES)
    model = make_pipeline("l2").fit(x,[0,0,1,1])
    before = model.named_steps["scale"].mean_.copy()
    x.loc[0] = [999,np.nan,np.nan,np.nan,999]
    assert np.isfinite(model.predict_proba(x)).all()
    np.testing.assert_array_equal(before,model.named_steps["scale"].mean_)


def test_saved_models_no_leakage_inference_shap_and_metrics():
    bundle = joblib.load(ROOT/"models/logistic_models.joblib")
    assert not set(bundle["train_sessions"]) & set(bundle["test_sessions"])
    assert not set(bundle["train_ids"]) & set(bundle["test_ids"])
    explainers = build_explainers(bundle)
    row = dict(avg_person=4,avg_objects=5,avg_noise=None,avg_voice=None,avg_speech=None)
    for name in bundle["models"]:
        result = infer(bundle,row,name)
        assert result["label"] == "grup"
        assert set(result["missing_features"]) == {"avg_noise","avg_voice","avg_speech"}
        values, logit = explain(bundle,explainers,row,name)
        assert len(values) == 5 and np.isfinite(logit)
    from sklearn.metrics import f1_score
    from rtap.storage import read_json
    from rtap.ml import illustrative_dataset
    data = illustrative_dataset()
    predictions = data[data.row_id.isin(bundle["test_ids"])].copy()
    for name, model in bundle["models"].items():
        predictions[name] = model.predict(predictions[FEATURES])
    for metric in read_json(ROOT/"reports/model_metrics.json")["models"]:
        assert metric["test_f1"] == pytest.approx(f1_score(predictions.label,predictions[metric["model"]]))
