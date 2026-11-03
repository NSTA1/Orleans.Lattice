using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A bootstrap must carry every row of a source whose access gate denies the
/// anonymous subject (issue #5683). The export is an infrastructure read that
/// runs with no user identity; it read its committed projection through the
/// client-facing scan, which a deny-by-default gate filters to nothing, so the
/// export streamed zero rows and the receiver held only what the incremental
/// stream re-shipped. A bulk load writes no WAL record, so its rows reach a
/// receiver only through the export: in the Contoso story the 240 orders A3
/// seeds through the Explorer's import never reached the second region. Two
/// real clusters: site A is the bootstrap source, with a deny-anonymous access
/// gate, site B the receiver, driven through the real bootstrap coordinator
/// over the real remote snapshot service.
/// </summary>
[TestFixture]
[Category("Integration")]
public class BulkLoadedTreeBootstrapIntegrationTests
{
    private const string SiteAClusterId = "blb-site-a";
    private const string SiteBClusterId = "blb-site-b";
    private const int SeededCount = 240;
    private const int WrittenCount = 10;

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private LatticeSnapshotProvider _siteAProvider = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        _siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions(SiteAClusterId));
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            _siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_siteB is not null)
        {
            await _siteB.StopAllSilosAsync();
            await _siteB.DisposeAsync();
        }

        if (_siteA is not null)
        {
            await _siteA.StopAllSilosAsync();
            await _siteA.DisposeAsync();
        }

        SiteATransports.TryRemove(SiteAClusterId, out _);
    }

    private static string SeededKey(int number) => string.Create(CultureInfo.InvariantCulture, $"order/{number:D6}");

    private static string WrittenKey(int number) => string.Create(CultureInfo.InvariantCulture, $"order/{100_000 + number:D6}");

    private static byte[] Value(string key) => System.Text.Encoding.UTF8.GetBytes(key);

    private static List<KeyValuePair<string, byte[]>> Seeded() =>
        [.. Enumerable.Range(1, SeededCount).Select(n => new KeyValuePair<string, byte[]>(SeededKey(n), Value(SeededKey(n))))];

    private async Task WriteAfterSeedAsync(ILattice tree)
    {
        for (var n = 1; n <= WrittenCount; n++)
        {
            await tree.SetAsync(WrittenKey(n), Value(WrittenKey(n)));
        }
    }

    private async Task BootstrapSiteBAsync(string tree)
    {
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        await coordinator.BootstrapAsync(SiteAClusterId, CancellationToken.None);

        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(250);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the bootstrap must complete");
    }

    private async Task<HashSet<string>> ExportedKeysAsync(string tree)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        await foreach (var entry in stream.Entries)
        {
            if (!entry.IsPrepared && !entry.IsTombstone && entry.Value is not null)
            {
                keys.Add(entry.Key);
            }
        }

        return keys;
    }

    private static async Task<List<string>> MissingAsync(ILattice tree, IEnumerable<string> keys)
    {
        var missing = new List<string>();
        foreach (var key in keys)
        {
            if (await tree.GetAsync(key) is null)
            {
                missing.Add(key);
            }
        }

        return missing;
    }

    private static IEnumerable<string> AllKeys() =>
        Enumerable.Range(1, SeededCount).Select(SeededKey).Concat(Enumerable.Range(1, WrittenCount).Select(WrittenKey));

    private async Task AssertSiteBHoldsEverythingAsync(string tree, string when)
    {
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        var missing = await MissingAsync(siteB, AllKeys());
        Assert.That(missing, Is.Empty,
            $"{when}: the receiver must hold every row of the source, the bulk-loaded ones included "
            + $"({missing.Count} missing, first {string.Join(", ", missing.Take(5))})");
        Assert.That(await siteB.CountAsync(), Is.EqualTo(SeededCount + WrittenCount), $"{when}: the receiver's live-key count");
    }

    [Test]
    public async Task Bootstrap_carries_the_rows_a_bulk_load_seeded_into_the_source()
    {
        const string tree = "blb-bulk-load";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await siteA.BulkLoadAsync(Seeded());
            await WriteAfterSeedAsync(siteA);
            Assert.That(await MissingAsync(siteA, AllKeys()), Is.Empty, "precondition: the source holds every row");
        }

        // Exported with no identity on the call, as a peer's export request is served.
        var exported = await ExportedKeysAsync(tree);
        Assert.That(AllKeys().Where(key => !exported.Contains(key)), Is.Empty, "the source's export carries every row");

        await BootstrapSiteBAsync(tree);
        await AssertSiteBHoldsEverythingAsync(tree, "after the bootstrap");
    }

    [Test]
    public async Task Bootstrap_carries_the_rows_a_chunked_bulk_append_seeded_into_the_source()
    {
        const string tree = "blb-bulk-append";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var seeded = Seeded();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            for (var chunk = 0; chunk * 60 < seeded.Count; chunk++)
            {
                await siteA.BulkAppendChunkAsync(
                    string.Create(CultureInfo.InvariantCulture, $"blb-append-{chunk}"),
                    seeded.Skip(chunk * 60).Take(60).ToList());
            }

            await WriteAfterSeedAsync(siteA);
            Assert.That(await MissingAsync(siteA, AllKeys()), Is.Empty, "precondition: the source holds every row");
        }

        await BootstrapSiteBAsync(tree);
        await AssertSiteBHoldsEverythingAsync(tree, "after the bootstrap");
    }

    [Test]
    public async Task A_re_bootstrap_over_a_populated_receiver_keeps_the_bulk_loaded_rows()
    {
        const string tree = "blb-re-bootstrap";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await siteA.BulkLoadAsync(Seeded());
            await WriteAfterSeedAsync(siteA);
        }

        await BootstrapSiteBAsync(tree);
        await AssertSiteBHoldsEverythingAsync(tree, "after the first bootstrap");

        // The story's receiver was re-seeded twice more after its first import.
        await BootstrapSiteBAsync(tree);
        await AssertSiteBHoldsEverythingAsync(tree, "after the re-bootstrap");
    }

    [Test]
    public async Task The_export_elevates_only_its_own_reads_and_a_read_its_consumer_makes_between_rows_stays_gated()
    {
        const string tree = "blb-consumer-gated";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await siteA.BulkLoadAsync(Seeded());
        }

        // A denied point read reports the key as absent.
        Assert.That(await siteA.GetAsync(SeededKey(1)), Is.Null, "precondition: an anonymous read of the source is refused");

        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        var rows = 0;
        var gatedBetweenRows = 0;
        await foreach (var entry in stream.Entries)
        {
            rows++;
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False,
                "the export's system-origin scope is not active while its consumer runs");

            // A consumer read between two rows is the anonymous caller's own, and stays gated.
            if (rows % 60 == 1 && await siteA.GetAsync(entry.Key) is null)
            {
                gatedBetweenRows++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo(SeededCount), "the export streams every row across many pages");
            Assert.That(gatedBetweenRows, Is.EqualTo((SeededCount + 59) / 60), "every consumer read between rows is refused");
        });
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
            siloBuilder.Services.AddSingleton<ILatticeAccessGate, DenyAnonymousAccessGate>();

            // Pages far smaller than the tree, so the export's committed scan crosses
            // many pages and the system-origin scope is re-entered across many yields.
            siloBuilder.ConfigureLattice(o => o.KeysPageSize = 16);
        }
    }

    /// <summary>A deny-by-default gate: the anonymous subject may do nothing.</summary>
    private sealed class DenyAnonymousAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            request.Subject.IsAnonymous
                ? new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("anonymous callers are denied"))
                : new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteBClusterId);

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
