using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryToolGroup"/>: it serves the telemetry group,
/// contributes the four read-only <c>lattice_telemetry_*</c> tools built once,
/// annotates every tool read-only and non-destructive, and excludes its
/// DI-injected collaborators (the backend client, metric-access policy, options,
/// and cancellation token) from every tool's input schema.
/// </summary>
[TestFixture]
public sealed class TelemetryToolGroupTests
{
    private static readonly string[] ExpectedToolNames =
    {
        "lattice_telemetry_query",
        "lattice_telemetry_query_range",
        "lattice_telemetry_list_metrics",
        "lattice_telemetry_metric_metadata",
    };

    private static TelemetryToolGroup CreateGroup()
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            BackendAddress = new Uri("https://prometheus.internal:9090/"),
        };
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IPrometheusQueryClient>());
        services.AddSingleton(new TelemetryMetricAccessPolicy(options));
        services.AddSingleton<IOptions<LatticeApiMcpTelemetryOptions>>(Options.Create(options));
        return new TelemetryToolGroup(services.BuildServiceProvider());
    }

    private static HashSet<string> InputSchemaPropertyNames(McpServerTool tool)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (tool.ProtocolTool.InputSchema.ValueKind == JsonValueKind.Object
            && tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var props)
            && props.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in props.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        return names;
    }

    [Test]
    public void Group_is_telemetry()
        => Assert.That(CreateGroup().Group, Is.EqualTo(LatticeApiMcpGroup.Telemetry));

    [Test]
    public void Tools_is_a_stable_non_null_instance()
    {
        var group = CreateGroup();
        Assert.That(group.Tools, Is.Not.Null.And.SameAs(group.Tools));
    }

    [Test]
    public void Contributes_exactly_the_four_telemetry_tools()
    {
        var names = CreateGroup().Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(ExpectedToolNames));
    }

    [Test]
    public void Every_tool_is_annotated_read_only_and_non_destructive()
    {
        var group = CreateGroup();

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                var annotations = tool.ProtocolTool.Annotations;
                Assert.That(annotations, Is.Not.Null, $"{tool.ProtocolTool.Name} must carry annotations.");
                Assert.That(annotations!.ReadOnlyHint, Is.True, $"{tool.ProtocolTool.Name} must be read-only.");
                Assert.That(annotations!.DestructiveHint, Is.False, $"{tool.ProtocolTool.Name} must be non-destructive.");
            }
        });
    }

    [Test]
    public void Injected_collaborators_are_excluded_from_every_tool_input_schema()
    {
        var group = CreateGroup();

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                var names = InputSchemaPropertyNames(tool);
                Assert.That(names, Does.Not.Contain("client"),
                    $"{tool.ProtocolTool.Name} must resolve the backend client from DI.");
                Assert.That(names, Does.Not.Contain("policy"),
                    $"{tool.ProtocolTool.Name} must resolve the metric-access policy from DI.");
                Assert.That(names, Does.Not.Contain("options"),
                    $"{tool.ProtocolTool.Name} must resolve its options from DI.");
                Assert.That(names, Does.Not.Contain("cancellationToken"),
                    $"{tool.ProtocolTool.Name} must not expose the cancellation token as an argument.");
            }
        });
    }

    [Test]
    public void The_instant_query_tool_exposes_the_query_argument()
    {
        var tool = CreateGroup().Tools.Single(t => t.ProtocolTool.Name == "lattice_telemetry_query");
        Assert.That(InputSchemaPropertyNames(tool), Does.Contain("query"));
    }

    [Test]
    public void The_range_query_tool_exposes_the_range_arguments()
    {
        var tool = CreateGroup().Tools.Single(t => t.ProtocolTool.Name == "lattice_telemetry_query_range");
        var names = InputSchemaPropertyNames(tool);
        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("query"));
            Assert.That(names, Does.Contain("start"));
            Assert.That(names, Does.Contain("end"));
            Assert.That(names, Does.Contain("step"));
        });
    }

    [Test]
    public void A_null_service_provider_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => new TelemetryToolGroup(services: null!));
}
