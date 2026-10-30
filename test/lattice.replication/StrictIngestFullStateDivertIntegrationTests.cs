using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for #5265: under strict ingest, a refused full-state CRDT row
/// (the bootstrap committed-projection form, with no typed delta) is diverted to
/// the schema dead-letter queue exactly once and the apply completes. The applier
/// installs such a row through a version-conditioned write, and the write choke
/// point used to report a dead-letter diversion as a version miss: the applier
/// retried it up to its compare-and-swap budget, dead-lettering it on every
/// attempt, then threw, and the dead-letter decorator retried the apply and parked
/// it in the replication dead-letter queue.
/// <para>
/// The interceptor here implements the strict-ingest contract of the schema write
/// interceptor at the core seam: a refused system-origin write records one
/// dead-letter entry and returns a dead-letter decision, and a refused local write
/// is rejected. The real canonical applier, dead-letter decorator, and grains run.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestFullStateDivertIntegrationTests
{
    private const string Origin = "sid-origin";
    private const string PnTree = "sid-pncounter";
    private const string OrMapTree = "sid-ormap";
    private const string RefusedPrefix = "refused-";
    private const int MaxApplyRetries = 3;

    private TestCluster _cluster = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;
    private ILatticeReplicationDeadLetters _replicationDeadLetters = null!;
    private RefusingStrictIngestInterceptor _interceptor = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _receiver = new DeadLetterTrackingReplicationApplier(
            services.GetRequiredService<ReplicationApplier>(),
            services.GetRequiredService<IGrainFactory>(),
            services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        _replicationDeadLetters = services.GetRequiredService<ILatticeReplicationDeadLetters>();
        _interceptor = (RefusingStrictIngestInterceptor)services.GetRequiredService<ILatticeWriteInterceptor>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static byte[] PnCounterState(long increment)
    {
        var counter = new PnCounter();
        counter.Increment("site-b", increment);
        return JsonLatticeSerializer<PnCounter>.Default.Serialize(counter);
    }

    private static byte[] OrMapState(long increment)
    {
        var counter = new PnCounter();
        counter.Increment("site-b", increment);
        var map = new OrMap<string, PnCounter>();
        map.Set("orders", "site-b", counter);
        return CrdtShape.ForOrMap<string, PnCounter>().SerializeState(map);
    }

    private static WalRecord FullStateRow(string tree, LatticeMergeMode mode, string key, byte[] state, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Mode = mode,
        Value = state,
        Delta = null,
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
    };

    /// <summary>
    /// Delivers <paramref name="entry"/> as the sender does: re-shipped until the
    /// receiver acknowledges it, up to the receiver's retry budget and then some.
    /// Returns the push on which it was acknowledged, or <c>0</c> if it never was.
    /// </summary>
    private async Task<int> DeliverAsync(WalRecord entry)
    {
        for (var attempt = 1; attempt <= MaxApplyRetries + 2; attempt++)
        {
            try
            {
                var result = await _receiver.ApplyAsync(entry);
                if (!result.Deferred)
                {
                    return attempt;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
            }
        }

        return 0;
    }

    private async Task AssertDivertedOnceAsync(string tree, string key, WalRecord row)
    {
        var ackedOnPush = await DeliverAsync(row);
        var stored = await _cluster.Client.GetGrain<ILattice>(tree).GetAsync(key);
        var replicationDeadLetters = await _replicationDeadLetters.CountAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(ackedOnPush, Is.EqualTo(1),
                "a refused row is diverted, so the apply completes on its first push instead of failing and being retried");
            Assert.That(_interceptor.DeadLettered(tree, key), Is.EqualTo(1),
                "a refused row is diverted to the schema dead-letter queue exactly once, not once per compare-and-swap attempt");
            Assert.That(replicationDeadLetters, Is.Zero,
                "a diverted row is not an apply failure, so it never reaches the replication dead-letter queue");
            Assert.That(stored, Is.Null, "a diverted row is not applied");
        });
    }

    [Test]
    public async Task Refused_full_state_pn_counter_row_under_strict_ingest_is_diverted_once_and_the_apply_completes()
    {
        const string key = RefusedPrefix + "pn";
        await AssertDivertedOnceAsync(
            PnTree, key, FullStateRow(PnTree, LatticeMergeMode.PnCounter, key, PnCounterState(4), 1_000));
    }

    [Test]
    public async Task Refused_full_state_or_map_row_under_strict_ingest_is_diverted_once_and_the_apply_completes()
    {
        const string key = RefusedPrefix + "ormap";
        await AssertDivertedOnceAsync(
            OrMapTree, key, FullStateRow(OrMapTree, LatticeMergeMode.OrMap, key, OrMapState(4), 1_000));
    }

    [Test]
    public async Task Accepted_full_state_row_under_strict_ingest_is_installed_with_no_dead_letter()
    {
        const string key = "accepted-pn";
        var state = PnCounterState(7);

        var ackedOnPush = await DeliverAsync(FullStateRow(PnTree, LatticeMergeMode.PnCounter, key, state, 2_000));
        var stored = await _cluster.Client.GetGrain<ILattice>(PnTree).GetAsync(key);

        Assert.Multiple(() =>
        {
            Assert.That(ackedOnPush, Is.EqualTo(1));
            Assert.That(_interceptor.DeadLettered(PnTree, key), Is.Zero);
            Assert.That(stored, Is.EqualTo(state), "an accepted row is installed verbatim");
        });
    }

    /// <summary>
    /// Strict ingest at the core seam: a system-origin write to a refused key is
    /// dead-lettered (one entry recorded per decision), a local one is rejected.
    /// </summary>
    private sealed class RefusingStrictIngestInterceptor : ILatticeWriteInterceptor
    {
        private readonly ConcurrentDictionary<(string Tree, string Key), int> _deadLettered = new();

        public bool InterceptsSystemOrigin => true;

        public int DeadLettered(string tree, string key) =>
            _deadLettered.TryGetValue((tree, key), out var count) ? count : 0;

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.Key.StartsWith(RefusedPrefix, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(LatticeWriteDecision.Accept());
            }

            if (!LatticeAccessGateContext.IsSystemOrigin)
            {
                return ValueTask.FromResult(LatticeWriteDecision.Reject("refused by policy"));
            }

            _deadLettered.AddOrUpdate((request.TreeId, request.Key), 1, (_, count) => count + 1);
            return ValueTask.FromResult(LatticeWriteDecision.DeadLetter("refused by policy"));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "sid-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.AddOrMapShape<string, PnCounter>(OrMapTree);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ModeResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(new RefusingStrictIngestInterceptor()));
        }
    }

    private sealed class ModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => treeId switch
        {
            PnTree => LatticeMergeMode.PnCounter,
            OrMapTree => LatticeMergeMode.OrMap,
            _ => LatticeMergeMode.LwwRegister,
        };
    }
}
