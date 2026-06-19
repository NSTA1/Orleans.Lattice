using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Single-cluster fixture for the materialised-view public API contract suite.
/// Brings up one silo registering <b>only</b> the public surface a local
/// materialised view needs - <c>AddLattice</c> (the WAL provider + commit-log
/// reader) and <c>AddLatticeViews</c> (which folds in the cursor registry) - and
/// deliberately <b>does not</b> call <c>AddLatticeReplication</c>. This both
/// exercises the public view API and proves that replication is not a runtime
/// prerequisite for a
/// <see cref="LatticeViewReplicationMode.DeriveLocally"/> view: a single-cluster
/// deployment needs a WAL provider, not a replicated cluster.
/// <para>
/// Every assertion in the suite drives convergence through the <b>public</b>
/// <see cref="ILatticeView"/> surface (<see cref="ILatticeView.WaitForSourceHeadAsync"/>,
/// <see cref="ILatticeView.RebuildAsync"/>), never the internal maintainer grain,
/// so the suite pins the contract callers actually depend on.
/// </para>
/// </summary>
internal sealed class MaterialisedViewPublicApiContractFixture
{
    /// <summary>Source tree id for the startup-declared filter view.</summary>
    public const string FilterSourceTreeId = "people";

    /// <summary>Startup-declared filter view name (keeps source keys with <c>Age &gt;= 18</c>).</summary>
    public const string FilterViewName = "adults";

    /// <summary>Source tree id for the startup-declared aggregation view.</summary>
    public const string AggregationSourceTreeId = "orders";

    /// <summary>Startup-declared aggregation view name (sum of order amount by customer).</summary>
    public const string AggregationViewName = "amount-by-customer";

    /// <summary>The single-silo test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The silo's service provider, used to resolve the silo-side public view factory.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>Stands up the cluster and waits for it to become ready.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Tears the cluster down.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>The contract record materialised through the filter view.</summary>
    public sealed record Person(int Age);

    /// <summary>The contract record reduced through the aggregation view.</summary>
    public sealed record Order(string Customer, double Amount);

    /// <summary>Serialises a <see cref="Person"/> the way the source tree stores it.</summary>
    public static byte[] PersonBytes(int age) =>
        JsonLatticeSerializer<Person>.Default.Serialize(new Person(age));

    /// <summary>Serialises an <see cref="Order"/> the way the source tree stores it.</summary>
    public static byte[] OrderBytes(string customer, double amount) =>
        JsonLatticeSerializer<Order>.Default.Serialize(new Order(customer, amount));

    /// <summary>The filter projection used by both the startup view and the runtime-created handle.</summary>
    public static PredicateLatticeViewProjection AdultFilter() =>
        new(LatticePredicateTranslator.Translate<Person>(p => p.Age >= 18));

    /// <summary>The aggregation projection used by both the startup view and the runtime-created handle.</summary>
    public static AggregationLatticeViewProjection AmountByCustomer() =>
        AggregationLatticeViewProjection.Create<Order>(
            AggregationKind.Sum,
            groupKeySelector: o => o.Customer,
            selectorVersion: "amount-by-customer-v1",
            valueSelector: o => o.Amount);

    /// <summary>Resolves the public <see cref="ILatticeView"/> handle for the filter view.</summary>
    public ILatticeView FilterView()
    {
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = Cluster.Client.GetGrain<ILattice>(FilterSourceTreeId);
        return factory.Create(source, FilterViewName, new LatticeViewDefinition(FilterViewName, AdultFilter()));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // NO AddLatticeReplication: only the public WAL + view surface.
            // AddLatticeViews folds in AddWalCursorRegistry, so a single-cluster
            // view needs only AddLattice + AddLatticeViews.
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views =>
            {
                views.AddView(FilterViewName, FilterSourceTreeId, AdultFilter());
                views.AddAggregationView(AggregationViewName, AggregationSourceTreeId, AmountByCustomer());
            });

            // A long coalesce window keeps the background drain timer dormant; the
            // suite drives convergence deterministically via the public
            // WaitForSourceHeadAsync barrier instead.
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
                o.CoalesceWindow = TimeSpan.FromMinutes(5));
        }
    }
}
