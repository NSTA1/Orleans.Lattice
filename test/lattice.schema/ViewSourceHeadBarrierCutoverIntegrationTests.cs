using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// <see cref="ILatticeView.WaitForSourceHeadAsync"/> is a read-your-writes barrier
/// through the source's <b>current</b> physical identity (#5308): after an eager
/// version migration cuts the source over to a re-stamped copy, the barrier returns
/// only once the view reflects the post-cutover source. A write the view applied from
/// the retired copy after the copy was built leaves the view's highest applied source
/// HLC above every entry of the new copy, so a barrier that compares HLC watermarks
/// returns while the view still holds pre-cutover membership.
/// <para>
/// The view's background drain timer is parked for the life of the fixture, so the
/// only drains are the ones the barrier itself drives: what the view shows after the
/// barrier depends on the barrier alone, never on timer scheduling.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewSourceHeadBarrierCutoverIntegrationTests
{
    private const uint SchemaId = 41;
    private const string SourceTreeId = "head-barrier-migrated";
    private const string ViewName = "head-barrier-migrated-tags";

    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    [Test]
    public async Task WaitForSourceHeadAsync_after_a_migration_cutover_reflects_post_cutover_membership()
    {
        var versionAdmin = SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();
        var tree = _cluster.Client.GetGrain<ILattice>(SourceTreeId);
        await versionAdmin.SetVersionConfigAsync(SourceTreeId, new LatticeSchemaVersionConfig(SchemaId, 1));
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        await tree.SetAsync("b", Json("""{"Amount":3}"""));

        var view = await SiloServices.GetRequiredService<ILatticeViewFactory>().GetAsync(ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAggregateInt64Async("Amount"), Is.EqualTo(2L),
            "precondition: both version-1 bodies are projected before the migration");

        var probe = SiloServices.GetRequiredService<CutoverProbe>();
        probe.Arm(SourceTreeId, view);
        var report = await versionAdmin.AdvanceAndMigrateAsync(SourceTreeId, 2);
        Assert.That(report.Succeeded, Is.True, "precondition: the migration cut the source over to its re-stamped copy");
        Assert.That(probe.AppliedBeforeCutover, Is.EqualTo(3L),
            "precondition: just before the alias swap the view applied a source write newer than the re-stamped copy");

        await view.WaitForSourceHeadAsync(Barrier);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await view.GetAggregateInt64Async("Total"), Is.EqualTo(2L),
                "the view must reflect the re-stamped (upcast) bodies of the post-cutover source");
            Assert.That(await view.GetAggregateInt64Async("Amount"), Is.Null,
                "the retired pre-cutover copy, including the write the swap superseded, must no longer contribute");
        });
    }

    /// <summary>The member name a body's shape carries (<c>Total</c>, <c>Amount</c> or <c>other</c>).</summary>
    private static string Shape(byte[] value)
    {
        var text = Encoding.UTF8.GetString(value);
        if (text.Contains("\"Total\"", StringComparison.Ordinal))
        {
            return "Total";
        }

        return text.Contains("\"Amount\"", StringComparison.Ordinal) ? "Amount" : "other";
    }

    /// <summary>Silo-scoped state shared between the test and the cutover filter.</summary>
    internal sealed class CutoverProbe
    {
        private volatile string? _armedSource;

        public ILatticeView? View { get; private set; }

        public long? AppliedBeforeCutover { get; set; }

        public void Arm(string sourceTreeId, ILatticeView view)
        {
            View = view;
            _armedSource = sourceTreeId;
        }

        public bool TryDisarm(string treeId) =>
            Interlocked.CompareExchange(ref _armedSource, null, treeId) is { } armed
            && string.Equals(armed, treeId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Just before the migration swaps the alias onto its re-stamped copy, writes one
    /// more value to the still-current source and lets the view apply it. The copy was
    /// built before that write, so the view's highest applied source HLC now sits above
    /// every entry of the copy - the state in which an HLC watermark cannot tell that
    /// the view still reflects the retired copy. Under the documented quiescence
    /// contract the write is superseded by the swap.
    /// </summary>
    private sealed class LateSourceWriteBeforeSwap(IGrainFactory grainFactory, CutoverProbe probe) : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.DeclaringType == typeof(ILatticeRegistry)
                && context.MethodName == nameof(ILatticeRegistry.SwapAliasAsync)
                && context.Request.GetArgumentCount() > 0
                && context.Request.GetArgument(0) is string treeId
                && probe.TryDisarm(treeId))
            {
                await grainFactory.GetGrain<ILattice>(treeId).SetAsync("c", Json("""{"Amount":1}"""));
                await probe.View!.WaitForSourceHeadAsync(Barrier);
                probe.AppliedBeforeCutover = await probe.View.GetAggregateInt64Async("Amount");
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<CutoverProbe>();
            siloBuilder.AddOutgoingGrainCallFilter<LateSourceWriteBeforeSwap>();
            siloBuilder.AddLatticeSchemaVersioning(registry => registry
                .AddSchema(SchemaId, 1, "barrier-v1")
                .AddSchema(SchemaId, 2, "barrier-v2")
                .AddUpcaster(
                    SchemaId, 1, 2,
                    LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("Amount", "Total"))));
            siloBuilder.AddLatticeViews(views => views.AddAggregationView(
                ViewName,
                SourceTreeId,
                new AggregationLatticeViewProjection(AggregationKind.Count, Shape, "barrier-tags-v1")));
            siloBuilder.ConfigureLatticeView(ViewName, o =>
            {
                o.CoalesceWindow = TimeSpan.FromHours(1);
                o.ReadHandleCacheTtl = TimeSpan.FromTicks(1);
            });
        }
    }
}
