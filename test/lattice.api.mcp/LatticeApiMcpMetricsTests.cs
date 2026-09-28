using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpMetrics"/>: its names, and the tags
/// <see cref="LatticeApiMcpMetrics.RecordToolClientError"/> emits (issue #3761).
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpMetricsTests
{
    [Test]
    public void Names_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeApiMcpMetrics.MeterName, Is.EqualTo("orleans.lattice.api.mcp"));
            Assert.That(LatticeApiMcpMetrics.Meter.Name, Is.EqualTo(LatticeApiMcpMetrics.MeterName));
            Assert.That(LatticeApiMcpMetrics.ToolClientErrorsName, Is.EqualTo("orleans.lattice.api.mcp.tool.client_errors"));
            Assert.That(LatticeApiMcpMetrics.ToolClientErrors.Name, Is.EqualTo(LatticeApiMcpMetrics.ToolClientErrorsName));
            Assert.That(LatticeApiMcpMetrics.ToolClientErrors.Meter, Is.SameAs(LatticeApiMcpMetrics.Meter));
            Assert.That(LatticeApiMcpMetrics.ToolClientErrors.Unit, Is.EqualTo("{error}"));
            Assert.That(LatticeApiMcpMetrics.TagTool, Is.EqualTo("tool"));
            Assert.That(LatticeApiMcpMetrics.TagReason, Is.EqualTo("reason"));
            Assert.That(LatticeApiMcpMetrics.ReasonInvalidArgument, Is.EqualTo("invalid_argument"));
            Assert.That(LatticeApiMcpMetrics.ReasonUnknownArgument, Is.EqualTo("unknown_argument"));
            Assert.That(LatticeApiMcpMetrics.ReasonRejectedContent, Is.EqualTo("rejected_content"));
            Assert.That(LatticeApiMcpMetrics.ReasonNotFound, Is.EqualTo("not_found"));
        });
    }

    [Test]
    public void RecordToolClientError_emits_one_measurement_tagged_by_tool_reason_and_platform_tenant()
    {
        var tool = "metrics_test_" + Guid.NewGuid().ToString("N");
        var captured = new List<Dictionary<string, object?>>();
        using var listener = MeterListening.StartForInstrument(LatticeApiMcpMetrics.ToolClientErrors, l =>
            l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var map = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var tag in tags)
                {
                    map[tag.Key] = tag.Value;
                }

                if (Equals(map.GetValueOrDefault(LatticeApiMcpMetrics.TagTool), tool))
                {
                    map["value"] = value;
                    lock (captured)
                    {
                        captured.Add(map);
                    }
                }
            }));

        LatticeApiMcpMetrics.RecordToolClientError(tool, McpToolClientErrorReason.RejectedContent);

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(captured[0]["value"], Is.EqualTo(1L));
            Assert.That(captured[0][LatticeApiMcpMetrics.TagReason], Is.EqualTo(LatticeApiMcpMetrics.ReasonRejectedContent));
            Assert.That(captured[0][LatticeTenantLabel.Platform.Key], Is.EqualTo(LatticeTenantLabel.Platform.Value));
        });
    }
}
