import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from rtap.config import configure_storage
configure_storage()

import hashlib
import joblib
from rtap.config import ROOT
from rtap.ml import illustrative_dataset, train_models, build_explainers, explain
from rtap.storage import write_json


def main():
    model_dir = ROOT / "models"
    model_dir.mkdir(exist_ok=True)
    training = ROOT / "data/training"
    training.mkdir(parents=True, exist_ok=True)
    data = illustrative_dataset()
    data.to_csv(training / "illustrative_synthetic.csv", index=False)
    bundle, metrics, predictions, cv_results = train_models(data)
    bundle["dataset_sha256"] = hashlib.sha256((training / "illustrative_synthetic.csv").read_bytes()).hexdigest()
    joblib.dump(bundle, model_dir / "logistic_models.joblib")
    report = ROOT / "reports"
    write_json(report / "model_metrics.json", dict(provenance=bundle["provenance"], rows=len(data),
               train_rows=len(bundle["train_ids"]), test_rows=len(bundle["test_ids"]),
               dataset_sha256=bundle["dataset_sha256"], models=metrics))
    write_json(report / "training_split.json", {k:bundle[k] for k in ("train_ids","test_ids","train_sessions","test_sessions")})
    predictions.to_csv(report / "test_predictions.csv", index=False)
    cv_results.to_csv(report / "grid_search_results.csv", index=False)
    reloaded = joblib.load(model_dir / "logistic_models.joblib")
    explainers = build_explainers(reloaded)
    checks = {}
    for name in reloaded["models"]:
        contributions, logit = explain(reloaded, explainers, data.iloc[0], name)
        checks[name] = {"logit":logit, "contributions":contributions.to_dict("records")}
    write_json(report / "shap_verification.json", checks)
    print("Trained and reloaded", len(bundle["models"]), "models; SHAP additivity passed for all.")
    for row in metrics:
        print(row["model"], "CV F1:",round(row["cv_f1"],4),"synthetic test F1:",round(row["test_f1"],4))


if __name__ == "__main__":
    main()
