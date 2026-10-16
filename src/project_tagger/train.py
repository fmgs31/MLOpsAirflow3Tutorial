"""Model training: a TF-IDF + SGD text classifier, tuned with Optuna, tracked in MLflow.

Small on purpose: the tutorial is about the MLOps around the model, and a fit
that takes a second lets you iterate on the pipeline instead of waiting on it.

Two tools carry the experiment:

* **Optuna** searches the hyperparameters. A *study* is one search; each
  *trial* tries one set of values and reports a score. The study lives in a
  database (OPTUNA_STORAGE), so several processes can run trials of the same
  study at once - which is what the mlops Dag does, one task per worker.
* **MLflow** records every run: its parameters, its metrics and, for the final
  model, the model file itself, which the registry (registry.py) then versions.
  A study is one parent run, and each of its trials a child run under it.
"""

from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import Any

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline

from project_tagger import config
from project_tagger import data as data_module
from project_tagger.evaluate import evaluate

DEFAULT_PARAMS: dict[str, Any] = {
    "ngram_max": 2,
    "min_df": 2,
    "alpha": 1e-4,
    "max_iter": 20,
    "seed": 42,
}


def build_pipeline(params: dict[str, Any]) -> Pipeline:
    params = {**DEFAULT_PARAMS, **params}
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=(1, int(params["ngram_max"])),
                    min_df=int(params["min_df"]),
                    sublinear_tf=True,
                ),
            ),
            (
                "clf",
                SGDClassifier(
                    loss="log_loss",
                    alpha=float(params["alpha"]),
                    max_iter=int(params["max_iter"]),
                    random_state=int(params["seed"]),
                    class_weight="balanced",
                ),
            ),
        ]
    )


def fit(
    train_df: pd.DataFrame, test_df: pd.DataFrame, params: dict[str, Any] | None = None
) -> tuple[Pipeline, dict[str, Any]]:
    """Fit on `train_df`, score on `test_df`. Returns (model, metrics). No tracking."""
    params = {**DEFAULT_PARAMS, **(params or {})}
    pipeline = build_pipeline(params)
    pipeline.fit(train_df["text"], train_df["tag"])
    metrics = evaluate(pipeline, test_df)
    metrics["params"] = params
    metrics["num_train"] = len(train_df)
    metrics["num_test"] = len(test_df)
    return pipeline, metrics


# --- MLflow -------------------------------------------------------------------


@contextmanager
def quiet_skops():
    """MLflow saves scikit-learn models with skops, which looks up every type it
    stores across all loaded modules. Inside an Airflow task that includes
    Airflow's deprecated aliases, each of which warns when touched - hundreds
    of lines of log noise per save or load. None of them is about this code.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


def setup_mlflow():
    import mlflow

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment(config.MLFLOW_EXPERIMENT)
    return mlflow


def flat_metrics(metrics: dict[str, Any]) -> dict[str, float]:
    """MLflow metrics are name -> number, so the nested per-class/slice dicts are flattened."""
    flat = {k: float(metrics[k]) for k in ("accuracy", "precision", "recall", "f1")}
    for tag, m in metrics.get("per_class", {}).items():
        flat[f"f1_{tag}"] = float(m["f1"])
    for name, m in metrics.get("slices", {}).items():
        flat[f"slice_{name}_f1"] = float(m["f1"])
    return flat


# --- Optuna -------------------------------------------------------------------


def create_study(study_name: str) -> str:
    """Create the study and its MLflow parent run, or load both if they exist -
    so a retried task does not fail, nor leave a second parent run behind.

    The parent run's id is kept on the study itself (a user attribute), where
    every trial, in whichever process it runs, can read it.
    """
    import optuna

    study = optuna.create_study(
        study_name=study_name,
        storage=config.OPTUNA_STORAGE,
        direction="maximize",
        load_if_exists=True,
    )
    if "mlflow_run_id" not in study.user_attrs:
        mlflow = setup_mlflow()
        with mlflow.start_run(run_name=study_name) as run:
            mlflow.set_tags({"optuna_study": study_name, "kind": "study"})
        study.set_user_attr("mlflow_run_id", run.info.run_id)
    return study_name


def objective(trial, train_df: pd.DataFrame, val_df: pd.DataFrame, study_name: str) -> float:
    """One trial: sample hyperparameters, fit, score on the validation split."""
    params = {
        "ngram_max": trial.suggest_int("ngram_max", 1, 3),
        "min_df": trial.suggest_int("min_df", 1, 3),
        "alpha": trial.suggest_float("alpha", 1e-6, 1e-2, log=True),
    }
    mlflow = setup_mlflow()
    with mlflow.start_run(
        run_name=f"{study_name}-trial-{trial.number}",
        parent_run_id=trial.study.user_attrs["mlflow_run_id"],
    ) as run:
        _, metrics = fit(train_df, val_df, params)
        mlflow.set_tags({"optuna_study": study_name, "kind": "trial"})
        mlflow.log_params(params)
        mlflow.log_metrics(flat_metrics(metrics))
        trial.set_user_attr("mlflow_run_id", run.info.run_id)
    return metrics["f1"]


def run_trials(study_name: str, n_trials: int, train_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Run `n_trials` of an existing study.

    The test split is never seen here: trials are scored on a validation split
    carved out of the training data, so the test score of the final model is
    still an honest estimate.
    """
    import optuna

    fit_df, val_df = data_module.split(train_df, test_size=0.2)
    study = optuna.load_study(study_name=study_name, storage=config.OPTUNA_STORAGE)
    before = {t.number for t in study.trials}
    study.optimize(lambda t: objective(t, fit_df, val_df, study_name), n_trials=n_trials)
    return [
        {"number": t.number, "f1": t.value, "params": t.params}
        for t in study.trials
        if t.number not in before and t.value is not None
    ]


def best_trial(study_name: str) -> dict[str, Any]:
    """The study's best trial, also recorded on the study's parent run.

    A metric and a tag, not params: MLflow params cannot change once logged,
    and a study that gets more trials (a cleared task) can get a new best.
    """
    import optuna

    study = optuna.load_study(study_name=study_name, storage=config.OPTUNA_STORAGE)
    best = study.best_trial
    trial_run_id = best.user_attrs["mlflow_run_id"]
    mlflow = setup_mlflow()
    with mlflow.start_run(run_id=study.user_attrs["mlflow_run_id"]):
        mlflow.log_metrics({"best_f1": best.value, "num_trials": len(study.trials)})
        mlflow.set_tag("best_trial_run_id", trial_run_id)
    return {
        "number": best.number,
        "f1": best.value,
        "params": best.params,
        "num_trials": len(study.trials),
        "mlflow_run_id": trial_run_id,
    }


def train_model(
    params: dict[str, Any],
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    run_name: str,
    tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fit the final model and log it - params, metrics, metrics.json and the model - to MLflow."""
    mlflow = setup_mlflow()
    import mlflow.sklearn

    with mlflow.start_run(run_name=run_name) as run:
        model, metrics = fit(train_df, test_df, params)
        mlflow.set_tags({"kind": "final", **(tags or {})})
        mlflow.log_params(metrics["params"])
        mlflow.log_metrics(flat_metrics(metrics))
        # The full nested metrics, for the evaluation and monitoring steps.
        mlflow.log_dict(metrics, "metrics.json")
        with quiet_skops():
            info = mlflow.sklearn.log_model(model, name="model")
    return {"run_id": run.info.run_id, "model_uri": info.model_uri, "metrics": metrics}


if __name__ == "__main__":
    # A quick fit on the warehouse table, outside any Dag and without tracking.
    df = data_module.preprocess(data_module.load_labeled())
    _, metrics = fit(*data_module.split(df))
    print(f"f1={metrics['f1']:.4f} accuracy={metrics['accuracy']:.4f}")
