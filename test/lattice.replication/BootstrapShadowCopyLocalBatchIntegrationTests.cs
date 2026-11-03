using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issues #5684 and #5689, the source half. A bootstrap import stages on a shadow copy of the
/// receiver's tree and then replaces the tree with that copy. Unlike the merge the
/// replication models assume (ReplicationReBootstrap's <c>ExportRow</c>), a cutover
/// keeps the receiver's own committed writes only if the copy carries them. That
/// covers a local atomic batch committed before the import, one committed while
/// the copy is held, and, in a mutual re-seed, a batch each side committed
/// before importing the other side's export.
/// <para>
/// In the Contoso run that exposed #5684, east's own atomic batch reached
/// strict west through no path. East re-seeded from west, so its tree became a
/// shadow copy, while west re-seeded from east's export of that copy. Each case
/// asserts the batch is readable through the logical tree after cutover, and that
/// the tree's next export carries it, since a re-seeding peer's import is
/// exactly that export.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BootstrapShadowCopyLocalBatchIntegrationTests
{
    private const string ClusterId = "scb-receiver";
    private const string Peer = "scb-peer";

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private TestCluster _cluster = null!;
    private ISnapshotProvider _exporter = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _exporter = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services
            .GetRequiredService<ISnapshotProvider>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        Source.ReleaseAll();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ReleaseGates() => Source.ReleaseAll();

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static string S(byte[]? bytes) => bytes is null ? "<absent>" : Encoding.UTF8.GetString(bytes);

    private ILattice Tree(string name) => _cluster.Client.GetGrain<ILattice>(name);

    private ILatticeBootstrapCoordinator Coordinator => new LatticeBootstrapCoordinator(_cluster.Client);

    /// <summary>A tree aliased to a populated copy, as a restored tree is (east's <c>bkprestore</c> copy).</summary>
    private async Task<string> AliasedTreeAsync(string name, params (string Key, string Value)[] rows)
    {
        var tree = $"{name}-{Guid.NewGuid():N}";
        var copy = tree + "-bkprestore";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry());
        await registry.RegisterAsync(copy, new TreeRegistryEntry());
        foreach (var (key, value) in rows)
        {
            await Tree(copy).SetAsync(key, V(value));
        }

        await registry.SetAliasAsync(tree, copy);
        return tree;
    }

    private Task CommitBatchAsync(string tree, params (string Key, string Value)[] entries) =>
        Tree(tree).SetManyAtomicAsync(
            [.. entries.Select(e => new KeyValuePair<string, byte[]>(e.Key, V(e.Value)))],
            "scb-batch-" + Guid.NewGuid().ToString("N"));

    private async Task<int> WaitLiveAsync(string tree, string source)
    {
        var failedAttempts = 0;
        var deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            var status = await Coordinator.GetStatusAsync(tree);
            if (status.Phase == LatticeBootstrapState.LiveIncremental)
            {
                return failedAttempts;
            }

            if (status.Phase == LatticeBootstrapState.Failed)
            {
                failedAttempts++;
                // A failed attempt (an export whose source moved under it) is
                // re-driven, as the sender's re-seed request re-drives it.
                await Coordinator.BootstrapAsync(tree, source);
            }

            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), $"the import into '{tree}' did not complete; last phase {status.Phase}");
            await Task.Delay(100);
        }
    }

    private async Task<Dictionary<string, string>> ExportedAsync(string tree)
    {
        var stream = await _exporter.ExportAsync(tree, HybridLogicalClock.Zero);
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        await foreach (var entry in stream.Entries)
        {
            if (!entry.IsPrepared && !entry.IsTombstone && !entry.IsDecision)
            {
                rows[entry.Key] = S(entry.Value);
            }
        }

        return rows;
    }

    private async Task AssertCarriesAsync(string tree, params (string Key, string Value)[] expected)
    {
        var read = await Tree(tree).GetManyAsync([.. expected.Select(e => e.Key)]);
        var exported = await ExportedAsync(tree);
        var physical = await _cluster.Client.GetLatticeRegistry().ResolveAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(physical, Does.Contain("/resized/"),
                $"PRECONDITION: '{tree}' was replaced by its bootstrap shadow copy");
            foreach (var (key, value) in expected)
            {
                Assert.That(S(read.GetValueOrDefault(key)), Is.EqualTo(value),
                    $"'{tree}' serves {key} after the cutover");
                Assert.That(exported.GetValueOrDefault(key), Is.EqualTo(value),
                    $"'{tree}''s next export carries {key}, so a peer re-seeding from it receives it");
            }
        });
    }

    [Test]
    public async Task A_local_atomic_batch_committed_before_the_import_survives_the_shadow_cutover_and_is_exported()
    {
        var tree = await AliasedTreeAsync("scb-before", ("order/1", "baseline"));
        await CommitBatchAsync(tree, ("order/1", "batch"), ("order/2", "batch"));
        Source.Fake(tree, gate: false, ("source/1", "from-peer"));

        await Coordinator.BootstrapAsync(tree, Peer);
        await WaitLiveAsync(tree, Peer);

        await AssertCarriesAsync(tree, ("order/1", "batch"), ("order/2", "batch"), ("source/1", "from-peer"));
    }

    [Test]
    public async Task A_local_atomic_batch_committed_while_the_shadow_copy_is_held_survives_the_cutover_and_is_exported()
    {
        var tree = await AliasedTreeAsync("scb-held", ("order/1", "baseline"));
        Source.Fake(tree, gate: true, ("source/1", "from-peer"), ("source/2", "from-peer"));

        await Coordinator.BootstrapAsync(tree, Peer);
        await Source.FirstRowAsync(tree, Patience);
        Assert.That(await _cluster.Client.GetGrain<ITreeResizeGrain>(tree).GetBootstrapCopyTreeIdAsync(), Is.Not.Null,
            "PRECONDITION: the import is draining into a held shadow copy");

        var batch = CommitBatchAsync(tree, ("order/1", "batch"), ("order/2", "batch"));
        Assert.That(await Task.WhenAny(batch, Task.Delay(Patience)), Is.SameAs(batch),
            "an atomic batch issued while the shadow copy is held completes");
        await batch;

        Source.Release(tree);
        await WaitLiveAsync(tree, Peer);

        await AssertCarriesAsync(
            tree, ("order/1", "batch"), ("order/2", "batch"), ("source/1", "from-peer"), ("source/2", "from-peer"));
    }

    [Test]
    public async Task Sequential_mutual_re_seeds_keep_each_sides_local_atomic_batch_on_both_sides()
    {
        var west = await AliasedTreeAsync("scb-seq-west", ("order/shared", "west-remediated"));
        var east = await AliasedTreeAsync("scb-seq-east", ("order/shared", "east-restored"));
        await CommitBatchAsync(east, ("order/shared", "east-batch"), ("order/east", "east-batch"));
        await CommitBatchAsync(west, ("order/west", "west-batch"));

        Source.FromPeer(west, peerTree: east, gate: false);
        Source.FromPeer(east, peerTree: west, gate: false);
        await Coordinator.BootstrapAsync(east, Peer);
        await WaitLiveAsync(east, Peer);
        await Coordinator.BootstrapAsync(west, Peer);
        await WaitLiveAsync(west, Peer);

        var expected = new[] { ("order/shared", "east-batch"), ("order/east", "east-batch"), ("order/west", "west-batch") };
        await AssertCarriesAsync(east, expected);
        await AssertCarriesAsync(west, expected);
    }

    [Test]
    public async Task Concurrent_mutual_re_seeds_keep_each_sides_local_atomic_batch_on_both_sides()
    {
        // West: the shared key holds an older (remediated) value. East: aliased to a
        // restored copy, then its own newer batch over the shared key and one of its
        // own, as C1's east ledger writes. Each re-seeds from the other's export.
        var west = await AliasedTreeAsync("scb-west", ("order/shared", "west-remediated"));
        var east = await AliasedTreeAsync("scb-east", ("order/shared", "east-restored"));
        await CommitBatchAsync(east, ("order/shared", "east-batch"), ("order/east", "east-batch"));
        await CommitBatchAsync(west, ("order/west", "west-batch"));

        Source.FromPeer(west, peerTree: east, gate: true);
        Source.FromPeer(east, peerTree: west, gate: true);
        await Coordinator.BootstrapAsync(west, Peer);
        await Coordinator.BootstrapAsync(east, Peer);
        await Source.FirstRowAsync(west, Patience);
        await Source.FirstRowAsync(east, Patience);
        var abortedCopy = await _cluster.Client.GetGrain<ITreeResizeGrain>(west).GetBootstrapCopyTreeIdAsync();

        // East cuts over first; west's export from east was opened before it, so
        // west's first attempt fails, its copy is aborted and the import is re-driven
        // into a new copy (#5689).
        Source.Release(east);
        await WaitLiveAsync(east, Peer);
        Source.Release(west);
        var westFailures = await WaitLiveAsync(west, Peer);
        var published = await _cluster.Client.GetLatticeRegistry().ResolveAsync(west);
        Assert.Multiple(() =>
        {
            Assert.That(westFailures, Is.GreaterThanOrEqualTo(1),
                "PRECONDITION: west's first attempt failed when east moved under its export");
            Assert.That(abortedCopy, Is.Not.Null);
            Assert.That(published, Is.Not.EqualTo(abortedCopy),
                "the re-driven import publishes a new copy, never the aborted copy's id");
        });

        var expected = new[] { ("order/shared", "east-batch"), ("order/east", "east-batch"), ("order/west", "west-batch") };
        await AssertCarriesAsync(east, expected);
        await AssertCarriesAsync(west, expected);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, Source>();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.AutoBootstrapOnFallOffLog = false;
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 1,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                };
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// The peer's export for each armed tree: either fixed rows, or the real export of
    /// another tree of this cluster standing in for the peer region's copy. A gated
    /// export pauses after its first row until released. Static because the silo
    /// constructs it.
    /// </summary>
    private sealed class Source(ISnapshotProvider exporter) : IBootstrapSnapshotSource
    {
        private static readonly ConcurrentDictionary<string, Arm> Arms = new(StringComparer.Ordinal);

        private sealed record Arm(string? PeerTree, (string Key, string Value)[] Rows, bool Gate)
        {
            public TaskCompletionSource FirstRow { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public static void Fake(string tree, bool gate, params (string Key, string Value)[] rows) =>
            Arms[tree] = new Arm(null, rows, gate);

        public static void FromPeer(string tree, string peerTree, bool gate) =>
            Arms[tree] = new Arm(peerTree, [], gate);

        public static void Release(string tree)
        {
            if (Arms.TryGetValue(tree, out var arm))
            {
                arm.Released.TrySetResult();
            }
        }

        public static void ReleaseAll()
        {
            foreach (var arm in Arms.Values)
            {
                arm.Released.TrySetResult();
            }
        }

        public static async Task FirstRowAsync(string tree, TimeSpan timeout)
        {
            var arm = Arms[tree];
            Assert.That(await Task.WhenAny(arm.FirstRow.Task, Task.Delay(timeout)), Is.SameAs(arm.FirstRow.Task),
                $"PRECONDITION: the import into '{tree}' applied its first row and paused");
        }

        public async Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            var arm = Arms[treeName];
            if (arm.PeerTree is { } peerTree)
            {
                var peer = await exporter.ExportAsync(peerTree, asOfHlc, cancellationToken);
                return new SnapshotStream(treeName, peer.AsOfHlc, peer.CausalStableFrontier, GateAsync(arm, peer.Entries));
            }

            var stamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 };
            return new SnapshotStream(treeName, asOfHlc, new VersionVector(), GateAsync(arm, FixedAsync(arm.Rows, stamp)));
        }

        private static async IAsyncEnumerable<SnapshotEntry> FixedAsync((string Key, string Value)[] rows, HybridLogicalClock stamp)
        {
            await Task.CompletedTask;
            foreach (var (key, value) in rows)
            {
                yield return new SnapshotEntry { Key = key, Value = V(value), Timestamp = stamp };
            }
        }

        private static async IAsyncEnumerable<SnapshotEntry> GateAsync(Arm arm, IAsyncEnumerable<SnapshotEntry> entries)
        {
            var first = true;
            await foreach (var entry in entries)
            {
                yield return entry;

                // The drain pulls the next row only after applying this one.
                if (first)
                {
                    first = false;
                    arm.FirstRow.TrySetResult();
                    if (arm.Gate)
                    {
                        await arm.Released.Task;
                    }
                }
            }

            arm.FirstRow.TrySetResult();
        }
    }
}
