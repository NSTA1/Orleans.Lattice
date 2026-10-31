using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with schema enforcement, schema versioning
/// and startup-declared materialised views, used to pin how the schema write path
/// and the projection input contract compose (#4908 section 4,
/// <c>spec/schema-view/SchemaViewComposition</c>). Each scenario owns a source tree
/// tailed by two views that reuse the projection-seam detectors' projections:
/// <list type="bullet">
/// <item><description><c>{source}-tags</c>, a count view grouping each value by what
/// the projection observed (envelope presence and body shape), so its groups count
/// each source key once per shape it was projected at; and</description></item>
/// <item><description><c>{source}-total</c>, a sum view over the target-shape
/// <c>Total</c> member.</description></item>
/// </list>
/// The registry declares one family with versions 1 to 3 and a single upcaster, 1 to
/// 2, that renames <c>Amount</c> to <c>Total</c>; there is deliberately no 2 to 3
/// hop, so a value stamped at version 1 or 2 cannot reach version 3.
/// </summary>
public sealed class SchemaViewCompositionClusterFixture
{
    /// <summary>The schema-family id the fixture's registry declares.</summary>
    public const uint SchemaId = 41;

    /// <summary>A source eagerly migrated after its target advances.</summary>
    public const string MigratedSource = "compose-migrate";

    /// <summary>A source whose migration aborts on a value it cannot upcast.</summary>
    public const string AbortedSource = "compose-abort";

    /// <summary>A source rewritten by a transform remediation.</summary>
    public const string RemediatedSource = "compose-remediate";

    /// <summary>A governed source that refuses a write and sees a retried one.</summary>
    public const string RefusedSource = "compose-refuse";

    /// <summary>A source that receives a value below the write check, through a tree merge.</summary>
    public const string MergedSource = "compose-merge";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side version admin.</summary>
    public ILatticeSchemaVersionAdmin VersionAdmin => SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    /// <summary>The silo-side policy admin.</summary>
    public ILatticeSchemaAdmin PolicyAdmin => SiloServices.GetRequiredService<ILatticeSchemaAdmin>();

    /// <summary>The silo-side remediation admin.</summary>
    public ILatticeSchemaRemediationAdmin RemediationAdmin => SiloServices.GetRequiredService<ILatticeSchemaRemediationAdmin>();

    /// <summary>The silo-side view factory.</summary>
    public ILatticeViewFactory ViewFactory => SiloServices.GetRequiredService<ILatticeViewFactory>();

    /// <summary>Returns the client-facing tree <paramref name="treeId"/>.</summary>
    public ILattice Source(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>The tags view name for <paramref name="source"/>.</summary>
    public static string TagsView(string source) => $"{source}-tags";

    /// <summary>The sum view name for <paramref name="source"/>.</summary>
    public static string TotalView(string source) => $"{source}-total";

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is null)
        {
            return;
        }

        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning(registry => registry
                .AddSchema(SchemaId, 1, "compose-v1")
                .AddSchema(SchemaId, 2, "compose-v2")
                .AddSchema(SchemaId, 3, "compose-v3")
                .AddUpcaster(
                    SchemaId, 1, 2,
                    LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("Amount", "Total"))));
            siloBuilder.AddLatticeViews(views =>
            {
                foreach (var source in new[] { MigratedSource, AbortedSource, RemediatedSource, RefusedSource, MergedSource })
                {
                    views.AddAggregationView(TagsView(source), source, ProjectionInputSeamClusterFixture.TagsProjection());
                    views.AddAggregationView(TotalView(source), source, ProjectionInputSeamClusterFixture.TotalProjection());
                }
            });
        }
    }
}
