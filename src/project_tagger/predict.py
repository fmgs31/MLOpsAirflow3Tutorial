"""Inference against whatever the registry currently calls production."""

from __future__ import annotations

from typing import Any

from project_tagger import registry


def predict(texts: list[str], run_id: str | None = None) -> list[dict[str, Any]]:
    if run_id is None:
        production = registry.get_production()
        if production is None:
            raise RuntimeError("No production model. Run the mlops Dag first.")
        run_id = production["run_id"]

    model = registry.load_model(run_id)
    probabilities = model.predict_proba(texts)
    classes = list(model.classes_)
    return [
        {
            "text": text,
            "tag": classes[int(row.argmax())],
            "confidence": float(row.max()),
            "run_id": run_id,
        }
        for text, row in zip(texts, probabilities)
    ]


if __name__ == "__main__":
    for prediction in predict(["Transfer learning with BERT for text classification"]):
        print(prediction)
