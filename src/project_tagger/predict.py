"""Inference with whatever the registry currently calls production."""

from __future__ import annotations

from typing import Any

from project_tagger import config, registry


def predict(texts: list[str], alias: str = config.CHAMPION_ALIAS) -> list[dict[str, Any]]:
    """Tag `texts` with the model the alias points at - `champion` unless told otherwise."""
    if registry.get_production() is None:
        raise RuntimeError("No production model. Run the mlops Dag first.")

    model = registry.load_model(alias)
    probabilities = model.predict_proba(texts)
    classes = list(model.classes_)
    return [
        {"text": text, "tag": classes[int(row.argmax())], "confidence": float(row.max())}
        for text, row in zip(texts, probabilities)
    ]


if __name__ == "__main__":
    for prediction in predict(["Transfer learning with BERT for text classification"]):
        print(prediction)
