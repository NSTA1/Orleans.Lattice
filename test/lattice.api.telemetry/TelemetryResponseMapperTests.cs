using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the projection of the backend envelope into the contract's series model:
/// the closed result-kind mapping, timestamp and value parsing, and the special
/// forms a backend can return.
/// </summary>
[TestFixture]
public sealed class TelemetryResponseMapperTests
{
    private static JsonElement Data(string json) => JsonDocument.Parse(json).RootElement.Clone();

    [Test]
    public void Map_projects_a_vector_into_one_single_sample_series_per_label_set()
    {
        var (kind, series) = TelemetryResponseMapper.Map(Data("""
            {
              "resultType": "vector",
              "result": [
                { "metric": { "tree": "orders" }, "value": [1767182400, "3"] },
                { "metric": { "tree": "users" }, "value": [1767182400, "4"] }
              ]
            }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(kind, Is.EqualTo(TelemetryResultKind.Vector));
            Assert.That(series, Has.Count.EqualTo(2));
            Assert.That(series[0].Points, Has.Count.EqualTo(1));
            Assert.That(series[0].Points[0].Value, Is.EqualTo(3d));
            Assert.That(series[1].TryGetLabel("tree", out var tree), Is.True);
            Assert.That(tree, Is.EqualTo("users"));
        });
    }

    [Test]
    public void Map_projects_a_matrix_into_one_multi_sample_series_per_label_set()
    {
        var (kind, series) = TelemetryResponseMapper.Map(Data("""
            {
              "resultType": "matrix",
              "result": [
                {
                  "metric": { "tree": "orders" },
                  "values": [[1767182400, "1"], [1767182460, "2"], [1767182520, "3"]]
                }
              ]
            }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(kind, Is.EqualTo(TelemetryResultKind.Matrix));
            Assert.That(series[0].Points, Has.Count.EqualTo(3));
            Assert.That(series[0].Points.Select(p => p.Value), Is.EqualTo(new[] { 1d, 2d, 3d }));
        });
    }

    [Test]
    public void Map_projects_a_scalar_into_a_single_label_free_series()
    {
        var (kind, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "scalar", "result": [1767182400, "42"] }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(kind, Is.EqualTo(TelemetryResultKind.Scalar));
            Assert.That(series, Has.Count.EqualTo(1));
            Assert.That(series[0].Labels, Is.Empty);
            Assert.That(series[0].Points[0].Value, Is.EqualTo(42d));
        });
    }

    [Test]
    public void Map_converts_a_prometheus_epoch_second_timestamp()
    {
        var (_, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "scalar", "result": [1767182400.5, "1"] }
            """));

        Assert.That(
            series[0].Points[0].Timestamp,
            Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1767182400500)));
    }

    [TestCase("NaN")]
    [TestCase("+Inf")]
    [TestCase("-Inf")]
    public void Map_carries_a_special_value_through_rather_than_coercing_it(string raw)
    {
        var (_, series) = TelemetryResponseMapper.Map(Data($$"""
            { "resultType": "scalar", "result": [1767182400, "{{raw}}"] }
            """));

        Assert.That(series[0].Points[0].IsFinite, Is.False,
            "A gap or an overflow must reach the client as itself.");
    }

    [Test]
    public void Map_reports_an_unrecognised_result_type_as_empty()
    {
        var (kind, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "string", "result": [1767182400, "hello"] }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(kind, Is.EqualTo(TelemetryResultKind.Empty));
            Assert.That(series, Is.Empty);
        });
    }

    [Test]
    public void Map_reports_a_malformed_payload_as_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryResponseMapper.Map(Data("[]")).Kind, Is.EqualTo(TelemetryResultKind.Empty));
            Assert.That(TelemetryResponseMapper.Map(Data("""{"resultType":"vector"}""")).Kind,
                Is.EqualTo(TelemetryResultKind.Empty));
            Assert.That(TelemetryResponseMapper.Map(default).Kind, Is.EqualTo(TelemetryResultKind.Empty));
        });
    }

    [Test]
    public void Map_skips_a_malformed_sample_rather_than_inventing_a_value()
    {
        var (_, series) = TelemetryResponseMapper.Map(Data("""
            {
              "resultType": "matrix",
              "result": [
                {
                  "metric": {},
                  "values": [[1767182400, "1"], [1767182460], ["bad", "2"], [1767182520, "not-a-number"]]
                }
              ]
            }
            """));

        Assert.That(series[0].Points, Has.Count.EqualTo(1),
            "Only the well-formed sample survives; the rest are dropped, never substituted.");
    }

    [Test]
    public void Map_reports_a_matched_series_with_no_samples_as_an_empty_point_list()
    {
        var (_, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "vector", "result": [ { "metric": { "tree": "orders" } } ] }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(series, Has.Count.EqualTo(1));
            Assert.That(series[0].Points, Is.Empty);
        });
    }

    [Test]
    public void Map_renders_a_non_string_label_value_as_its_raw_json()
    {
        var (_, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "vector", "result": [ { "metric": { "shard": 3 }, "value": [1, "1"] } ] }
            """));

        Assert.That(series[0].TryGetLabel("shard", out var shard), Is.True);
        Assert.That(shard, Is.EqualTo("3"));
    }

    [Test]
    public void Map_reads_a_numeric_sample_value()
    {
        var (_, series) = TelemetryResponseMapper.Map(Data("""
            { "resultType": "scalar", "result": [1767182400, 7.25] }
            """));

        Assert.That(series[0].Points[0].Value, Is.EqualTo(7.25));
    }

    [TestCase(1.7e12, Description = "milliseconds where the protocol specifies seconds")]
    [TestCase(1e18, Description = "beyond long range entirely")]
    [TestCase(-1e18, Description = "negative and beyond long range")]
    [TestCase(-1e12, Description = "further back than the representable minimum")]
    public void Map_skips_a_timestamp_outside_the_representable_range(double epochSeconds)
    {
        // A backend emitting milliseconds where seconds are specified is the realistic
        // case. Converting without a range check would throw out of the mapper, past
        // the fault handling that turns a bad payload into a backend exception.
        var (_, series) = TelemetryResponseMapper.Map(Data($$"""
            {
              "resultType": "matrix",
              "result": [ { "metric": {}, "values": [[{{epochSeconds}}, "1"], [1767182400, "2"]] } ]
            }
            """));

        Assert.Multiple(() =>
        {
            Assert.That(series[0].Points, Has.Count.EqualTo(1),
                "The unrepresentable sample is skipped like any other unreadable one.");
            Assert.That(series[0].Points[0].Value, Is.EqualTo(2d));
        });
    }

    [Test]
    public void Map_never_throws_on_an_out_of_range_timestamp()
    {
        Assert.That(
            () => TelemetryResponseMapper.Map(Data("""
                { "resultType": "scalar", "result": [1.7976931348623157e308, "1"] }
                """)),
            Throws.Nothing);
    }
}
