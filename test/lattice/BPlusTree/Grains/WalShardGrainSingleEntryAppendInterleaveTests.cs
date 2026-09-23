using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Establishes what <see cref="LatticeOptions.WalBatchedSingleEntryAppends"/>
/// does and does not change about observable WAL shard behaviour, so the
/// option's default can rest on evidence rather than on caution.
/// </summary>
/// <remarks>
/// <para>
/// The concern the option was created to hedge was that routing a one-entry
/// bulk append onto the interleaving <see cref="IWalShardGrain.AppendBatchAsync"/>
/// would weaken a read guarantee held by the shard's non-interleaving
/// readers. These tests pin down that it does not, and pin down the one thing
/// it genuinely does change.
/// </para>
/// <para>
/// The load-bearing fact is structural: Orleans interleaves only at
/// <c>await</c> boundaries, and the two in-memory readers hold no
/// <c>await</c> at all, so there is no point at which they could be
/// interleaved regardless of what else is queued on the activation.
/// <see cref="NoYieldPointGuard"/> enforces that rather than asserting it in
/// prose, so a future edit that introduces an <c>await</c> - and with it a
/// real interleaving point - fails here instead of silently acquiring one.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public class WalShardGrainSingleEntryAppendInterleaveTests
{
    private const string ClusterId = "wal-single-entry-site";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        GatingAppendWalStorageProvider.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ResetGate() => GatingAppendWalStorageProvider.Reset();

    /// <summary>
    /// The structural reason the exclusive turn conferred no read guarantee
    /// worth keeping: a method with no <c>await</c> has no yield point, so
    /// Orleans has nowhere to interleave it and its single <c>long</c> read
    /// cannot be observed torn - whatever the concurrency of the append path
    /// around it.
    /// </summary>
    [Test]
    public void NoYieldPointGuard_in_memory_readers_have_no_await_so_cannot_interleave()
    {
        var readers = new[]
        {
            nameof(IWalShardGrain.GetNextSequenceAsync),
#pragma warning disable CS0618 // Deliberately guarding the obsolete member too; it is still callable.
            nameof(IWalShardGrain.GetEntryCountAsync),
#pragma warning restore CS0618
        };

        Assert.Multiple(() =>
        {
            foreach (var name in readers)
            {
                var method = typeof(WalShardGrain).GetMethod(
                    name, BindingFlags.Public | BindingFlags.Instance);

                Assert.That(method, Is.Not.Null, $"{name} must exist on WalShardGrain.");
                Assert.That(
                    method!.GetCustomAttribute<AsyncStateMachineAttribute>(),
                    Is.Null,
                    $"{name} must contain no await. An await would give Orleans a yield point to " +
                    "interleave at, which is the only way a concurrent append could affect what " +
                    "this reader observes. If this fails, the safety argument for " +
                    "WalBatchedSingleEntryAppends defaulting on must be re-derived, not suppressed.");
            }
        });
    }

    /// <summary>
    /// The one thing the option genuinely changes, measured directly: with a
    /// provider append parked mid-flight, an exclusive-turn append holds the
    /// activation and a reader queues behind it, whereas the interleaving
    /// batched route lets the reader through.
    /// </summary>
    /// <remarks>
    /// This is a strict improvement and the same property
    /// <see cref="IWalShardGrain.ReadAsync"/> already relies on, but it is a
    /// real behavioural difference and is asserted as one rather than being
    /// characterised as a no-op.
    /// </remarks>
    [Test]
    public async Task Reader_is_head_of_line_blocked_by_a_parked_exclusive_append_only()
    {
        var blockedWhenExclusive = await ReaderBlocksWhileAppendParkedAsync(
            tree: "single-entry-exclusive", batchedSingleEntryAppends: false);
        var blockedWhenBatched = await ReaderBlocksWhileAppendParkedAsync(
            tree: "single-entry-batched", batchedSingleEntryAppends: true);

        Assert.Multiple(() =>
        {
            Assert.That(blockedWhenExclusive, Is.True,
                "With the option off a one-entry bulk append takes the exclusive overload, so a " +
                "reader must queue behind the parked provider round trip. If this stops being " +
                "true the option no longer has a control arm and the A/B is meaningless.");
            Assert.That(blockedWhenBatched, Is.False,
                "With the option on the append interleaves, so the reader must complete while the " +
                "provider round trip is still parked.");
        });
    }

    /// <summary>
    /// Pins the actual durability boundary, which is not where the option's
    /// original safety argument assumed it was: an append that has been
    /// assigned an offset but has not yet committed in the provider is
    /// visible to the next-sequence reader - on both routes, because both
    /// assign under the same state gate before the provider round trip - yet
    /// is invisible to the entry readers, which bound themselves by the
    /// durable contiguous tail instead.
    /// </summary>
    /// <remarks>
    /// This is why the exclusive turn was never load-bearing.
    /// <c>GetNextSequenceAsync</c> reports the assignment tail by design;
    /// <c>DurableContiguousTailOffset</c> is the durability signal, and
    /// <see cref="IWalShardGrain.ReadAsync"/> and
    /// <see cref="IWalShardGrain.ReadShippingAsync"/> already use it
    /// precisely so they never surface an offset above a still-in-flight
    /// lower one. Exclusivity only made the assignment window harder to
    /// observe for one-entry appends; it never made it absent, and
    /// multi-entry appends have always exposed it.
    /// </remarks>
    [Test]
    public async Task Uncommitted_append_advances_the_sequence_but_stays_invisible_to_entry_readers()
    {
        const string tree = "single-entry-coherence";
        var grain = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        await grain.AppendBatchAsync(new[] { MakeEntry(tree, "seed") }, CancellationToken.None);
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        GatingAppendWalStorageProvider.Arm(tree);
        try
        {
            var appendTask = grain.AppendBatchAsync(
                new[] { MakeEntry(tree, "parked") }, CancellationToken.None);
            await GatingAppendWalStorageProvider.AppendEntered!.Task
                .WaitAsync(TimeSpan.FromSeconds(15));

            var during = await grain.GetNextSequenceAsync(CancellationToken.None)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            var page = await grain.ReadAsync(0, 100, CancellationToken.None)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Multiple(() =>
            {
                Assert.That(during, Is.EqualTo(before + 1),
                    "GetNextSequenceAsync reports the assignment tail, which advances under the " +
                    "state gate before the provider commits. This is the documented semantic on " +
                    "both append routes, so interleaving does not change what it means.");
                Assert.That(page.Entries, Has.Count.EqualTo(1),
                    "The entry readers bound themselves by the durable contiguous tail, so a " +
                    "parked append must not be surfaced. This is the invariant that actually " +
                    "protects consumers, and it is unaffected by which overload was used.");
            });

            GatingAppendWalStorageProvider.Release();
            await appendTask.WaitAsync(TimeSpan.FromSeconds(15));
        }
        finally
        {
            GatingAppendWalStorageProvider.Release();
        }

        var after = await grain.GetNextSequenceAsync(CancellationToken.None);
        var finalPage = await grain.ReadAsync(0, 100, CancellationToken.None)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before + 1),
                "The committed append leaves the sequence where assignment already put it.");
            Assert.That(finalPage.Entries, Has.Count.EqualTo(2),
                "Once committed the append becomes visible to the entry readers.");
        });
    }

    /// <summary>
    /// The correctness property interleaving could plausibly break, and the
    /// one the shard's internal state gate exists to preserve: concurrent
    /// one-entry bulk appends must still receive dense, unique, strictly
    /// ascending offsets with none lost or duplicated.
    /// </summary>
    [Test]
    public async Task Concurrent_single_entry_appends_keep_offsets_dense_and_unique_when_batched()
    {
        const string tree = "single-entry-density";
        const int appendCount = 64;
        var grain = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        var tasks = Enumerable
            .Range(0, appendCount)
            .Select(i => grain.AppendBatchAsync(
                new[] { MakeEntry(tree, $"k{i}") }, CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
        var offsets = results.SelectMany(r => r).OrderBy(o => o).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Has.Length.EqualTo(appendCount),
                "Every concurrent append must yield exactly one offset.");
            Assert.That(offsets.Distinct().Count(), Is.EqualTo(appendCount),
                "Interleaved appends must never share an offset.");
            Assert.That(offsets, Is.EqualTo(Enumerable.Range(0, appendCount).Select(i => (long)i)),
                "Offsets must remain dense and gap-free under interleaved assignment.");
        });

        var next = await grain.GetNextSequenceAsync(CancellationToken.None);
        Assert.That(next, Is.EqualTo((long)appendCount),
            "The next sequence must account for every interleaved append exactly once.");
    }

    private async Task<bool> ReaderBlocksWhileAppendParkedAsync(
        string tree, bool batchedSingleEntryAppends)
    {
        var grain = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        // Seed through the batched route so the seed itself is never the
        // call under observation.
        await grain.AppendBatchAsync(new[] { MakeEntry(tree, "seed") }, CancellationToken.None);

        GatingAppendWalStorageProvider.Arm(tree);
        Task appendTask;
        try
        {
            appendTask = AppendOneAsync(grain, tree, "parked", batchedSingleEntryAppends);
            await GatingAppendWalStorageProvider.AppendEntered!.Task
                .WaitAsync(TimeSpan.FromSeconds(15));

            var readerTask = grain.GetNextSequenceAsync(CancellationToken.None).AsTask();
            var winner = await Task.WhenAny(readerTask, Task.Delay(TimeSpan.FromSeconds(3)));
            return winner != readerTask;
        }
        finally
        {
            GatingAppendWalStorageProvider.Release();
        }
    }

    private static Task AppendOneAsync(
        IWalShardGrain grain, string tree, string key, bool batchedSingleEntryAppends) =>
        batchedSingleEntryAppends
            ? grain.AppendBatchAsync(new[] { MakeEntry(tree, key) }, CancellationToken.None)
            : grain.AppendAsync(MakeEntry(tree, key), CancellationToken.None);

    private static WalRecord MakeEntry(string tree, string key) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = ClusterId,
    };

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<IWalStorageProvider>(
                    _ => new GatingAppendWalStorageProvider()));
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Decorates <see cref="InMemoryWalStorageProvider"/> and, when armed for
    /// a specific tree, parks the provider append on a gate so a test can
    /// hold a write open and observe whether a concurrent reader is
    /// head-of-line blocked behind the grain turn that issued it.
    /// </summary>
    private sealed class GatingAppendWalStorageProvider : IWalStorageProvider
    {
        private readonly IWalStorageProvider _inner = new InMemoryWalStorageProvider();

        internal static volatile TaskCompletionSource? AppendGate;
        internal static volatile TaskCompletionSource? AppendEntered;
        private static volatile string? _gatedTree;

        internal static void Arm(string tree)
        {
            _gatedTree = tree;
            AppendEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            AppendGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        internal static void Release() => AppendGate?.TrySetResult();

        internal static void Reset()
        {
            _gatedTree = null;
            AppendGate = null;
            AppendEntered = null;
        }

        private static async Task GateAsync(string treeId, CancellationToken cancellationToken)
        {
            var gate = AppendGate;
            if (gate is not null && string.Equals(treeId, _gatedTree, StringComparison.Ordinal))
            {
                AppendEntered?.TrySetResult();
                await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task AppendBatchAsync(
            string treeId, int shardIndex, IReadOnlyList<WalEntry> entries,
            CancellationToken cancellationToken)
        {
            await GateAsync(treeId, cancellationToken).ConfigureAwait(false);
            await _inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task AppendEncodedBatchAsync(
            string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
            ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder,
            CancellationToken cancellationToken)
        {
            await GateAsync(treeId, cancellationToken).ConfigureAwait(false);
            await _inner.AppendEncodedBatchAsync(
                    treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken)
                .ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            CancellationToken cancellationToken) =>
            _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(
            string treeId, int shardIndex, long upToOffsetInclusive,
            CancellationToken cancellationToken) =>
            _inner.TrimAsync(treeId, shardIndex, upToOffsetInclusive, cancellationToken);

        public Task EvaluateCompactionAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);

        public Task ReconcileAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.ReconcileAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetRetainedByteSizeAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetPhysicalByteSizeAsync(
            string treeId, int shardIndex, CancellationToken cancellationToken) =>
            _inner.GetPhysicalByteSizeAsync(treeId, shardIndex, cancellationToken);
    }
}
