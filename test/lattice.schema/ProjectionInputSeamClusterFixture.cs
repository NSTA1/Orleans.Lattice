using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with schema versioning and startup-declared
/// materialised views, used to pin the projection input contract (#4908, #5240):
/// which bytes <see cref="ILatticeAggregationProjection.Project"/> and
/// <see cref="ILatticeViewProjection.Project"/> receive when their source tree is
/// versioning-enabled. Each scenario owns a source tree tailed by three views:
/// <list type="bullet">
/// <item><description><c>{source}-tags</c>, a count view that groups each value by
/// what the projection observed (envelope presence, stamped version and body
/// shape), so the view's rows record the projection's input;</description></item>
/// <item><description><c>{source}-total</c>, a sum view whose selector reads the
/// target-shape <c>Total</c> member as an envelope-unaware JSON reader; and</description></item>
/// <item><description><c>{source}-filter</c>, a predicate filter view keeping values
/// with <c>Total &gt;= 0</c>.</description></item>
/// </list>
/// The versioning registry declares one family with versions 1 and 2; the 1 to 2
/// upcaster renames <c>Amount</c> to <c>Total</c>.
/// </summary>
public sealed class ProjectionInputSeamClusterFixture
{
    /// <summary>The schema-family id the fixture's registry declares.</summary>
    public const uint SchemaId = 31;

    /// <summary>A versioned source whose views are only ever maintained incrementally.</summary>
    public const string IncrementalSource = "seam-incremental";

    /// <summary>A versioned source whose views are rebuilt and reconciled.</summary>
    public const string RebuildSource = "seam-rebuild";

    /// <summary>A versioned source written through atomic batches (atomic publication).</summary>
    public const string AtomicSource = "seam-atomic";

    /// <summary>
    /// A versioned logical source aliased to a physical tree before its first write,
    /// so a rebuild reads the physical shards directly.
    /// </summary>
    public const string AliasedSource = "seam-aliased";

    /// <summary>A source with versioning registered but never opted in (the control).</summary>
    public const string PlainSource = "seam-plain";

    /// <summary>The single group the sum view accumulates into.</summary>
    public const string TotalGroup = "all";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side version admin.</summary>
    public ILatticeSchemaVersionAdmin VersionAdmin => SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    /// <summary>The silo-side view factory.</summary>
    public ILatticeViewFactory ViewFactory => SiloServices.GetRequiredService<ILatticeViewFactory>();

    /// <summary>Returns the client-facing tree <paramref name="treeId"/>.</summary>
    public ILattice Source(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>The tags view name for <paramref name="source"/>.</summary>
    public static string TagsView(string source) => $"{source}-tags";

    /// <summary>The sum view name for <paramref name="source"/>.</summary>
    public static string TotalView(string source) => $"{source}-total";

    /// <summary>The filter view name for <paramref name="source"/>.</summary>
    public static string FilterView(string source) => $"{source}-filter";

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

    /// <summary>
    /// Names what a projection observed: <c>enveloped:v{version}:{shape}</c> for a
    /// value carrying the version envelope, or <c>plain:{shape}</c> otherwise, where
    /// the shape is the body's <c>Total</c> or <c>Amount</c> member (or <c>other</c>).
    /// </summary>
    internal static string Tag(byte[] value)
    {
        if (LatticeSchemaEnvelope.TryReadHeader(value, out _, out var version))
        {
            return $"enveloped:v{version}:{Shape(LatticeSchemaEnvelope.StripToBody(value))}";
        }

        return $"plain:{Shape(value)}";
    }

    /// <summary>
    /// Reads the target-shape <c>Total</c> member the way an envelope-unaware JSON
    /// projection would: bytes that do not parse as a JSON object contribute zero.
    /// </summary>
    internal static double TotalOf(byte[] value) =>
        TryParseObject(value, out var root) && root.TryGetProperty("Total", out var total)
            && total.ValueKind == JsonValueKind.Number
            ? total.GetDouble()
            : 0d;

    /// <summary>The count projection grouping each value by <see cref="Tag"/>.</summary>
    internal static ILatticeAggregationProjection TagsProjection() =>
        new AggregationLatticeViewProjection(AggregationKind.Count, Tag, "seam-tags-v1");

    /// <summary>The sum projection over the target-shape <c>Total</c> member.</summary>
    internal static ILatticeAggregationProjection TotalProjection() =>
        new AggregationLatticeViewProjection(
            AggregationKind.Sum, static _ => TotalGroup, "seam-total-v1", valueSelector: TotalOf);

    /// <summary>The predicate filter projection keeping <c>Total &gt;= 0</c>.</summary>
    internal static ILatticeViewProjection FilterProjection() =>
        new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<SeamDocument>(d => d.Total >= 0));

    private static string Shape(byte[] body)
    {
        if (!TryParseObject(body, out var root))
        {
            return "other";
        }

        if (root.TryGetProperty("Total", out _))
        {
            return "Total";
        }

        return root.TryGetProperty("Amount", out _) ? "Amount" : "other";
    }

    private static bool TryParseObject(byte[] value, out JsonElement root)
    {
        try
        {
            using var document = JsonDocument.Parse(value);
            if (document.RootElement.ValueKind == JsonValueKind.Object)
            {
                root = document.RootElement.Clone();
                return true;
            }
        }
        catch (JsonException)
        {
        }

        root = default;
        return false;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaVersioning(registry => registry
                .AddSchema(SchemaId, 1, "seam-v1")
                .AddSchema(SchemaId, 2, "seam-v2")
                .AddUpcaster(
                    SchemaId, 1, 2,
                    LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("Amount", "Total"))));
            siloBuilder.AddLatticeViews(views =>
            {
                foreach (var source in new[] { IncrementalSource, RebuildSource, AtomicSource, AliasedSource, PlainSource })
                {
                    views.AddAggregationView(TagsView(source), source, TagsProjection());
                    views.AddAggregationView(TotalView(source), source, TotalProjection());
                    views.AddView(FilterView(source), source, FilterProjection());
                }
            });
        }
    }
}

/// <summary>The target (version 2) document shape the filter view's predicate reads.</summary>
/// <param name="Total">The numeric member the version 1 to 2 upcaster renames <c>Amount</c> to.</param>
public sealed record SeamDocument(double Total);
