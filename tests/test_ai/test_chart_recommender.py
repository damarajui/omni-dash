"""Tests for the chart recommender rule engine."""

from omni_dash.ai.chart_recommender import (
    FieldInfo,
    classify_field,
    recommend_chart,
    _infer_format,
)


def _fi(name: str, ftype: str = "number", measure: bool = True, dim: bool = False) -> FieldInfo:
    return FieldInfo(name=name, field_type=ftype, is_measure=measure, is_dimension=dim)


class TestRecommendChart:
    def test_single_measure_returns_kpi(self):
        fields = [_fi("t.revenue")]
        rec = recommend_chart(fields)
        assert rec.chart_type == "number"
        assert rec.confidence >= 0.9

    def test_date_plus_measures_returns_line(self):
        fields = [
            _fi("t.date", "date", measure=False, dim=True),
            _fi("t.visits"),
            _fi("t.clicks"),
        ]
        rec = recommend_chart(fields)
        assert rec.chart_type == "line"
        assert "t.date" in (rec.vis_config.get("x_axis", ""))

    def test_date_measure_categorical_returns_line_color(self):
        fields = [
            _fi("t.date", "date", measure=False, dim=True),
            _fi("t.revenue"),
            _fi("t.channel", "string", measure=False, dim=True),
        ]
        rec = recommend_chart(fields)
        assert rec.chart_type == "line"
        assert rec.vis_config.get("color_by") == "t.channel"

    def test_categorical_plus_measure_returns_bar(self):
        fields = [
            _fi("t.country", "string", measure=False, dim=True),
            _fi("t.sales"),
        ]
        rec = recommend_chart(fields)
        assert rec.chart_type == "bar"

    def test_categorical_plus_multi_measures_returns_grouped(self):
        fields = [
            _fi("t.channel", "string", measure=False, dim=True),
            _fi("t.revenue"),
            _fi("t.cost"),
        ]
        rec = recommend_chart(fields)
        assert rec.chart_type == "grouped_bar"

    def test_two_measures_returns_scatter(self):
        fields = [_fi("t.x"), _fi("t.y")]
        rec = recommend_chart(fields)
        assert rec.chart_type == "scatter"

    def test_date_measure_multi_dims_returns_stacked_area(self):
        fields = [
            _fi("t.date", "date", measure=False, dim=True),
            _fi("t.spend"),
            _fi("t.channel", "string", measure=False, dim=True),
        ]
        rec = recommend_chart(fields)
        assert rec.chart_type in ("line", "stacked_area")

    def test_empty_fields_returns_table(self):
        rec = recommend_chart([])
        assert rec.chart_type == "table"

    def test_alternatives_populated(self):
        fields = [_fi("t.revenue")]
        rec = recommend_chart(fields)
        assert isinstance(rec.alternatives, list)
        assert len(rec.alternatives) > 0


class TestClassifyField:
    def test_date_field(self):
        fi = classify_field({"name": "t.date", "type": "date"})
        assert fi.field_type == "date"
        assert fi.is_dimension is True

    def test_number_field(self):
        fi = classify_field({"name": "t.revenue", "type": "number"})
        assert fi.field_type == "number"
        assert fi.is_measure is True

    def test_string_field(self):
        fi = classify_field({"name": "t.name", "type": "string"})
        assert fi.field_type == "string"
        assert fi.is_dimension is True

    def test_id_suffix_is_dimension(self):
        fi = classify_field({"name": "t.user_id", "type": "number"})
        assert fi.is_dimension is True
        assert fi.is_measure is False

    def test_aggregated_field_is_measure(self):
        fi = classify_field({"name": "t.total", "type": "number", "aggregation": "sum"})
        assert fi.is_measure is True


class TestInferFormat:
    def test_revenue_gets_currency(self):
        assert _infer_format("t.revenue") == "USDCURRENCY_0"

    def test_rate_gets_percent(self):
        assert _infer_format("t.conversion_rate") == "PERCENT_1"

    def test_count_gets_bignumber(self):
        assert _infer_format("t.user_count") == "BIGNUMBER_0"

    def test_unknown_returns_none(self):
        assert _infer_format("t.foobar") is None
