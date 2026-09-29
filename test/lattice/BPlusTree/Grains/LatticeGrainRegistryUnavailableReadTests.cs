using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3641. Deterministic discriminators for the multi-key read paths under
/// a registry that fails in transport. The lattice grain is built in-process
/// over a substituted shard root that simulates two leaves of one saga - one
/// drained (serving the post-saga value, no prepare left), one undrained
/// (holding the prepare) - and answers exactly as a real leaf does for each
/// registry ambient: against the stamped snapshot, by throwing
/// <see cref="LatticeTransactionOutcomeUnavailableException"/> under the
/// "snapshot unavailable" marker, or by resolving the saga live at its own
/// moment when no scope is set. The leaf half of that contract is pinned
/// against the real <see cref="BPlusLeafGrain"/> in
/// <c>BPlusLeafGrainTests.SnapshotUnavailable</c>.
/// <para>
/// Before the fix every registry call failure on this path read as "stable", so
/// (a) and (b) returned a torn result - one key post-saga, the other pre-saga -
/// and (c) resolved per leaf. The guards pass on both arms: with no prepared
/// key, a read completes while the registry is unreachable.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeGrainRegistryUnavailableReadTests
{
    private const string TreeId = "registry-unavailable-read";
    private static readonly byte[] Pre = [1];
    private static readonly byte[] Post = [2];

    /// <summary>
    /// The simulated leaves and the registry decision they resolve against.
    /// </summary>
    private sealed class SimulatedSaga
    {
        public Guid TxId { get; } = Guid.NewGuid();

        /// <summary>The registry's recorded decision.</summary>
        public bool Committed { get; set; }

        /// <summary>Keys whose leaf still holds the saga's prepare.</summary>
        public HashSet<string> Undrained { get; } = new(StringComparer.Ordinal);

        /// <summary>Keys whose leaf drained the committed saga into its entries.</summary>
        public HashSet<string> Drained { get; } = new(StringComparer.Ordinal);

        /// <summary>Invoked before each key is read, to interleave a commit.</summary>
        public Action<string>? BeforeKey { get; set; }

        public int ShardReads { get; private set; }

        public void CommitAndDrain(string key)
        {
            Committed = true;
            Undrained.Remove(key);
            Drained.Add(key);
        }

        public Dictionary<string, byte[]> ReadMany(IEnumerable<string> keys)
        {
            ShardReads++;
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                BeforeKey?.Invoke(key);
                result[key] = ReadKey(key);
            }

            return result;
        }

        public byte[] ReadKey(string key)
        {
            if (Drained.Contains(key)) return Post;
            if (!Undrained.Contains(key)) return Pre;

            if (LatticeRegistrySnapshotContext.IsUnavailable)
            {
                throw LatticeTransactionOutcomeUnavailableException.Create(TreeId, null, 1, [TxId], null);
            }

            var status = LatticeRegistrySnapshotContext.Current is { } snap
                ? (snap.TryGetValue(TxId, out var s) ? s : TxStatus.InFlight)
                : (Committed ? TxStatus.Committed : TxStatus.InFlight);
            return status == TxStatus.Committed ? Post : Pre;
        }
    }

    private sealed class Harness
    {
        public LatticeGrain Grain { get; set; } = null!;
        public required IShardRootGrain Shard { get; init; }
        public required SimulatedSaga Saga { get; init; }

        /// <summary>Answers the snap1 / disambiguation fetch; throw to fail it.</summary>
        public Func<TxRegistrySnapshot> Snapshot { get; set; } = () => new TxRegistrySnapshot
        {
            Decisions = new Dictionary<Guid, TxStatus>(),
            Revision = 1,
        };

        /// <summary>Answers the post-fan-out revision probe; throw to fail it.</summary>
        public Func<long> Revision { get; set; } = () => 1;
    }

    private static Harness CreateHarness(int maxScanRetries = 3)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new LatticeOptions { WalPartitions = 1, MaxScanRetries = maxScanRetries };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var latticeRegistry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(latticeRegistry);
        latticeRegistry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        latticeRegistry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        latticeRegistry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var saga = new SimulatedSaga();
        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shard);
        shard.GetManyAsync(Arg.Any<List<string>>())
            .Returns(c => Task.FromResult(saga.ReadMany(c.Arg<List<string>>())));
        shard.CountBoundedAsync(Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                var values = saga.ReadMany(["a", "b"]);
                var count = 0;
                foreach (var v in values.Values) if (v == Post) count++;
                return Task.FromResult(new ShardCountPage { Count = count });
            });
        shard.GetSortedKeysBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                // A key page resolves every key it visits exactly as a leaf does.
                saga.ReadMany(["a", "b"]);
                return Task.FromResult(new KeysPage { Keys = ["a", "b"], HasMore = false });
            });

        var harness = new Harness
        {
            Shard = shard,
            Saga = saga,
        };

        var txRegistry = Substitute.For<ITxRegistryGrain>();
        grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(txRegistry);
        txRegistry.SnapshotWithRevisionAsync().Returns(_ => Task.FromResult(harness.Snapshot()));
        txRegistry.GetDecisionsRevisionAsync().Returns(_ => Task.FromResult(harness.Revision()));
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.GetShardHighWaterAsync().Returns(Task.FromResult(0));
        grainFactory.GetGrain<ITxRegistryHighWaterGrain>(Arg.Any<string>()).Returns(highWater);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);
        var services = Substitute.For<IServiceProvider>();
        harness.Grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return harness;
    }

    // ---- (a) snap1 OK, the probe fails, the saga commits mid-fan-out ----

    [Test]
    public async Task GetManyAsync_with_a_failed_probe_after_a_mid_fan_out_commit_never_returns_a_torn_read()
    {
        var h = CreateHarness();
        h.Saga.Undrained.UnionWith(["a", "b"]);
        var attempt = 0;
        h.Snapshot = () =>
        {
            attempt++;
            // Once the registry recovers it reports the saga committed.
            return new TxRegistrySnapshot
            {
                Decisions = h.Saga.Committed
                    ? new Dictionary<Guid, TxStatus> { [h.Saga.TxId] = TxStatus.Committed }
                    : new Dictionary<Guid, TxStatus>(),
                Revision = h.Saga.Committed ? 2 : 1,
            };
        };
        // The commit lands mid-fan-out on the first attempt: key "a"'s leaf
        // drains before it is read, key "b"'s leaf still holds the prepare and
        // resolves it against snap1's InFlight. The probe then fails, once.
        h.Saga.BeforeKey = key =>
        {
            if (key == "a" && !h.Saga.Committed) h.Saga.CommitAndDrain("a");
        };
        var probeCalls = 0;
        h.Revision = () =>
        {
            if (probeCalls++ == 0) throw new TimeoutException("registry probe timed out");
            return h.Saga.Committed ? 2 : 1;
        };

        var result = await h.Grain.GetManyAsync(["a", "b"]);

        Assert.That(result["a"], Is.EqualTo(result["b"]),
            "a failed probe must not certify a read in which one key is post-saga and the other pre-saga");
        Assert.That(result["a"], Is.EqualTo(Post));
        Assert.That(attempt, Is.GreaterThan(1), "the unverifiable attempt must be retried under a fresh snapshot");
    }

    [Test]
    public void GetManyAsync_with_a_persistently_failing_probe_over_a_prepared_key_throws_outcome_unavailable()
    {
        var h = CreateHarness(maxScanRetries: 2);
        h.Saga.Undrained.Add("b");
        h.Revision = () => throw new TimeoutException("registry probe timed out");

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await h.Grain.GetManyAsync(["a", "b"])) as LatticeTransactionOutcomeUnavailableException;

        Assert.That(ex!.TreeId, Is.EqualTo(TreeId));
        Assert.That(ex.TransactionIds, Does.Contain(h.Saga.TxId));
        Assert.That(ex.InnerException, Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            "the leaf's own report is carried as the inner exception");
    }

    // ---- (b) snap1 fails, leaves hold prepares ----

    [Test]
    public void GetManyAsync_with_a_failed_snapshot_over_prepared_keys_throws_outcome_unavailable_after_retries()
    {
        var h = CreateHarness(maxScanRetries: 3);
        h.Saga.Undrained.UnionWith(["a", "b"]);
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");
        // Resolving live per leaf, the commit would fall between the two keys.
        h.Saga.BeforeKey = key =>
        {
            if (key == "b") h.Saga.Committed = true;
        };

        Assert.That(async () => await h.Grain.GetManyAsync(["a", "b"]),
            Throws.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            "with no single decision view, a read that depends on a saga must fail closed, not resolve per leaf");
        Assert.That(h.Saga.ShardReads, Is.EqualTo(3), "each attempt within MaxScanRetries is tried once");
    }

    [Test]
    public void CountAsync_with_a_failed_snapshot_over_prepared_keys_throws_outcome_unavailable()
    {
        var h = CreateHarness(maxScanRetries: 2);
        h.Saga.Undrained.UnionWith(["a", "b"]);
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");

        Assert.That(async () => await h.Grain.CountAsync(),
            Throws.TypeOf<LatticeTransactionOutcomeUnavailableException>());
    }

    // ---- (c) streaming scan with a failed snapshot hitting a prepared key ----

    [Test]
    public void KeysAsync_with_a_failed_scan_snapshot_hitting_a_prepared_key_throws_outcome_unavailable()
    {
        var h = CreateHarness();
        h.Saga.Undrained.Add("b");
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");

        Assert.That(async () => await DrainAsync(h.Grain.KeysAsync()),
            Throws.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            "a failed scan-start snapshot must not leave every page resolving its prepares per leaf");
    }

    [Test]
    public async Task KeysAsync_with_a_failed_scan_snapshot_and_no_prepared_key_still_succeeds()
    {
        var h = CreateHarness();
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");

        Assert.That(await DrainAsync(h.Grain.KeysAsync()), Is.EqualTo(new[] { "a", "b" }));
    }

    private static async Task<List<string>> DrainAsync(IAsyncEnumerable<string> source)
    {
        var result = new List<string>();
        await foreach (var k in source) result.Add(k);
        return result;
    }

    // ---- guards: no prepared key, registry unreachable ----

    [Test]
    public async Task GetManyAsync_with_an_unreachable_registry_and_no_prepared_key_still_succeeds()
    {
        var h = CreateHarness();
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");
        h.Revision = () => throw new TimeoutException("registry probe timed out");

        var result = await h.Grain.GetManyAsync(["a", "b"]);

        Assert.That(result["a"], Is.EqualTo(Pre));
        Assert.That(result["b"], Is.EqualTo(Pre));
        Assert.That(h.Saga.ShardReads, Is.EqualTo(1), "no retry: nothing depended on the registry");
    }

    [Test]
    public async Task GetManyAsync_with_a_failed_probe_and_no_prepared_key_still_succeeds()
    {
        var h = CreateHarness();
        h.Revision = () => throw new TimeoutException("registry probe timed out");

        var result = await h.Grain.GetManyAsync(["a", "b"]);

        Assert.That(result.Values, Is.All.EqualTo(Pre));
    }

    [Test]
    public async Task CountAsync_with_an_unreachable_registry_and_no_prepared_key_still_succeeds()
    {
        var h = CreateHarness();
        h.Snapshot = () => throw new TimeoutException("registry snapshot timed out");
        h.Revision = () => throw new TimeoutException("registry probe timed out");

        Assert.That(await h.Grain.CountAsync(), Is.Zero);
    }

    [Test]
    public async Task GetManyAsync_with_a_healthy_registry_takes_one_pass()
    {
        var h = CreateHarness();
        h.Saga.Undrained.Add("b");

        var result = await h.Grain.GetManyAsync(["a", "b"]);

        Assert.That(result["b"], Is.EqualTo(Pre), "InFlight under snap1 falls through to the pre-saga value");
        Assert.That(h.Saga.ShardReads, Is.EqualTo(1), "healthy-registry behaviour and retry counts are unchanged");
    }

    [Test]
    public void GetManyAsync_with_a_non_transport_snapshot_fault_propagates_it()
    {
        var h = CreateHarness();
        h.Snapshot = () => throw new InvalidDataException("registry bug");

        Assert.That(async () => await h.Grain.GetManyAsync(["a"]), Throws.TypeOf<InvalidDataException>());
    }
}
