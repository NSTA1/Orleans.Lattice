using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Cluster fixture that stands up a silo with materialised views declared at
/// startup via <c>AddLatticeViews</c>, so the view maintainer, factory, and
/// activation service are all exercised end-to-end. Each view tails its own
/// dedicated source tree so per-view value formats stay independent.
/// </summary>
public sealed class ViewClusterFixture
{
    public const string CountView = "agg-count";
    public const string SumView = "agg-sum";
    public const string MinView = "agg-min";
    public const string MaxView = "agg-max";
    public const string SetUnionView = "agg-setunion";
    public const string FilterView = "filter-view";
    public const string FoldView = "fold-view";

    public const string CountSource = "src-count";
    public const string SumSource = "src-sum";
    public const string MinSource = "src-min";
    public const string MaxSource = "src-max";
    public const string SetUnionSource = "src-setunion";
    public const string FilterSource = "src-filter";
    public const string FoldSource = "src-fold";

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>Returns a source-tree grain reference by tree id.</summary>
    public ILattice Source(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>Resolves the silo-side <see cref="ILatticeViewFactory"/>.</summary>
    public ILatticeViewFactory ViewFactory =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<ILatticeViewFactory>();

    /// <summary>Encodes a "group|numeric|member" aggregation source value.</summary>
    public static byte[] AggValue(string group, double numeric = 0, string member = "") =>
        Encoding.UTF8.GetBytes($"{group}|{numeric.ToString(System.Globalization.CultureInfo.InvariantCulture)}|{member}");

    internal static string GroupOf(byte[] v) => Encoding.UTF8.GetString(v).Split('|')[0];

    internal static double NumericOf(byte[] v) =>
        double.Parse(Encoding.UTF8.GetString(v).Split('|')[1], System.Globalization.CultureInfo.InvariantCulture);

    internal static string MemberOf(byte[] v) => Encoding.UTF8.GetString(v).Split('|')[2];

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views =>
            {
                views.AddAggregationView(
                    CountView, CountSource,
                    new AggregationLatticeViewProjection(AggregationKind.Count, GroupOf, "v1"));
                views.AddAggregationView(
                    SumView, SumSource,
                    new AggregationLatticeViewProjection(AggregationKind.Sum, GroupOf, "v1", valueSelector: NumericOf));
                views.AddAggregationView(
                    MinView, MinSource,
                    new AggregationLatticeViewProjection(AggregationKind.Min, GroupOf, "v1", valueSelector: NumericOf));
                views.AddAggregationView(
                    MaxView, MaxSource,
                    new AggregationLatticeViewProjection(AggregationKind.Max, GroupOf, "v1", valueSelector: NumericOf));
                views.AddAggregationView(
                    SetUnionView, SetUnionSource,
                    new AggregationLatticeViewProjection(AggregationKind.SetUnion, GroupOf, "v1", memberSelector: MemberOf));
                views.AddView(
                    FilterView, FilterSource,
                    new PredicateLatticeViewProjection(
                        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18)));
                views.AddFoldedView(
                    FoldView, FoldSource,
                    new LatticeFoldProjection(
                        GroupOf,
                        () => BitConverter.GetBytes(0L),
                        (acc, _, _, _) => BitConverter.GetBytes(BitConverter.ToInt64(acc) + 1L),
                        "v1"));
            });
        }
    }
}

/// <summary>Simple POCO used by the filter view's predicate.</summary>
public sealed record ViewPerson(string Name, int Age);
