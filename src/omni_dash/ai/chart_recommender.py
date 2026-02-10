"""Rule-based chart type recommendation engine.

Analyzes field types and counts to recommend the best chart type
for a given data shape. Used by the suggest_chart MCP tool.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldInfo:
    """Metadata about a single field."""

    name: str
    field_type: str = "unknown"  # "date", "number", "string", "boolean"
    is_measure: bool = False
    is_dimension: bool = False


@dataclass
class ChartRecommendation:
    """A chart type recommendation with reasoning."""

    chart_type: str
    confidence: float  # 0.0 - 1.0
    reasoning: str
    vis_config: dict[str, Any] = field(default_factory=dict)
    alternatives: list[str] = field(default_factory=list)


def classify_field(field_info: dict[str, Any]) -> FieldInfo:
    """Classify a field from Omni topic metadata into FieldInfo."""
    name = field_info.get("name", "")
    ftype = field_info.get("type", "").lower()
    aggregation = field_info.get("aggregation", "")
    label = field_info.get("label", "").lower()
    raw_name = name.split(".")[-1].lower() if "." in name else name.lower()

    # Determine field type
    if ftype in ("date", "datetime", "timestamp", "time"):
        field_type = "date"
    elif ftype in ("number", "integer", "float", "decimal", "numeric", "int"):
        field_type = "number"
    elif ftype in ("boolean", "bool"):
        field_type = "boolean"
    else:
        field_type = "string"

    # Determine if measure or dimension
    is_measure = bool(aggregation) or field_type == "number"
    is_dimension = not is_measure

    # Heuristic: fields ending with _id, _name, _type, _status are dimensions
    dim_suffixes = ("_id", "_name", "_type", "_status", "_category", "_label", "_code")
    if any(raw_name.endswith(s) for s in dim_suffixes):
        is_dimension = True
        is_measure = False

    # Heuristic: date fields are dimensions
    if field_type == "date":
        is_dimension = True
        is_measure = False

    return FieldInfo(
        name=name,
        field_type=field_type,
        is_measure=is_measure,
        is_dimension=is_dimension,
    )


def recommend_chart(fields: list[FieldInfo]) -> ChartRecommendation:
    """Recommend a chart type based on the data shape.

    Rules:
    1. 1 measure, no dimensions → number/KPI
    2. 1 date + 1-3 measures → line
    3. 1 date + 1 measure + 1 categorical → line with color_by
    4. 1 categorical + 1 measure → bar
    5. 1 categorical + 2+ measures → grouped_bar
    6. 2 measures only → scatter
    7. date + measures showing composition → stacked_area
    8. Fallback → table
    """
    dates = [f for f in fields if f.field_type == "date"]
    measures = [f for f in fields if f.is_measure]
    dimensions = [f for f in fields if f.is_dimension and f.field_type != "date"]

    n_dates = len(dates)
    n_measures = len(measures)
    n_dims = len(dimensions)

    # Rule 1: Single measure, no context → KPI
    if n_measures == 1 and n_dates == 0 and n_dims == 0:
        return ChartRecommendation(
            chart_type="number",
            confidence=0.95,
            reasoning="Single measure with no dimensions — best as a KPI number tile.",
            vis_config={"value_format": _infer_format(measures[0].name)},
            alternatives=["table"],
        )

    # Rule 2: Date + measures → line chart
    if n_dates >= 1 and 1 <= n_measures <= 3 and n_dims == 0:
        return ChartRecommendation(
            chart_type="line",
            confidence=0.90,
            reasoning=f"Time series with {n_measures} measure(s) — line chart shows trends clearly.",
            vis_config={
                "x_axis": dates[0].name,
                "y_axis": [m.name for m in measures],
            },
            alternatives=["area", "bar"],
        )

    # Rule 3: Date + measure + categorical → line with color_by
    if n_dates >= 1 and n_measures >= 1 and n_dims == 1:
        return ChartRecommendation(
            chart_type="line",
            confidence=0.85,
            reasoning="Time series with a categorical dimension — line chart colored by category.",
            vis_config={
                "x_axis": dates[0].name,
                "y_axis": [measures[0].name],
                "color_by": dimensions[0].name,
            },
            alternatives=["stacked_area", "grouped_bar"],
        )

    # Rule 4: Single categorical + single measure → bar
    if n_dims == 1 and n_measures == 1 and n_dates == 0:
        return ChartRecommendation(
            chart_type="bar",
            confidence=0.90,
            reasoning="One dimension and one measure — bar chart for comparison.",
            vis_config={
                "x_axis": dimensions[0].name,
                "y_axis": [measures[0].name],
            },
            alternatives=["pie", "table"],
        )

    # Rule 5: Single categorical + multiple measures → grouped bar
    if n_dims == 1 and n_measures >= 2 and n_dates == 0:
        return ChartRecommendation(
            chart_type="grouped_bar",
            confidence=0.85,
            reasoning=f"One dimension with {n_measures} measures — grouped bar for side-by-side comparison.",
            vis_config={
                "x_axis": dimensions[0].name,
                "y_axis": [m.name for m in measures],
            },
            alternatives=["stacked_bar", "table"],
        )

    # Rule 6: Two measures, no dims → scatter
    if n_measures == 2 and n_dims == 0 and n_dates == 0:
        return ChartRecommendation(
            chart_type="scatter",
            confidence=0.80,
            reasoning="Two measures with no dimensions — scatter plot shows correlation.",
            vis_config={
                "x_axis": measures[0].name,
                "y_axis": [measures[1].name],
            },
            alternatives=["table", "line"],
        )

    # Rule 7: Date + measures + categorical → stacked area (composition)
    if n_dates >= 1 and n_measures >= 1 and n_dims >= 1:
        return ChartRecommendation(
            chart_type="stacked_area",
            confidence=0.75,
            reasoning="Time series with categorical breakdown — stacked area shows composition over time.",
            vis_config={
                "x_axis": dates[0].name,
                "y_axis": [measures[0].name],
                "color_by": dimensions[0].name,
                "stacked": True,
            },
            alternatives=["stacked_bar", "line", "table"],
        )

    # Fallback: table
    return ChartRecommendation(
        chart_type="table",
        confidence=0.60,
        reasoning="Complex field mix — table provides the most flexible view.",
        vis_config={},
        alternatives=["bar", "line"],
    )


def _infer_format(field_name: str) -> str | None:
    """Infer a value format from a field name."""
    name = field_name.lower().split(".")[-1] if "." in field_name else field_name.lower()

    if any(kw in name for kw in ("revenue", "arr", "cost", "spend", "price", "amount", "cac", "ltv")):
        return "USDCURRENCY_0"
    if any(kw in name for kw in ("rate", "percent", "ctr", "cvr", "ratio", "pct")):
        return "PERCENT_1"
    if any(kw in name for kw in ("count", "total", "num", "sum")):
        return "BIGNUMBER_0"
    return None
