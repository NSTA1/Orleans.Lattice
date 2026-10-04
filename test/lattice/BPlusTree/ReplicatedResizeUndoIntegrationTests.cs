using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4518, on real grains: a resize of a replicated tree cannot be undone
/// once the resized copy has served it. The shipper tails that copy's log, so
/// writes it took may already be on a peer, and cross-cluster shipping is
/// last-writer-wins and never retracts them; the undo would discard them on this
/// cluster and leave them on the peer. The undo is refused before any
/// compensation runs, leaving the tree on the resized copy. An undo before the
/// swap, and any undo of an unreplicated tree, still runs.
/// <para>
/// Replication is represented by its core seam alone,
/// <see cref="ILatticeReplicationContext"/>, which reports a merge mode for every
/// tree whose id starts with <see cref="ReplicatedPrefix"/>.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ReplicatedResizeUndoIntegrationTests
{
    private const string ReplicatedPrefix = "repl-";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        SliceGate.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private ILatticeRegistry Registry => _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task An_undo_after_the_swap_of_a_replicated_tree_is_refused_and_leaves_it_on_the_resized_copy()
    {
        var treeId = $"{ReplicatedPrefix}undo-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("before", Bytes("1"));
        await ResizeToCompletionAsync(tree);
        var resized = await Registry.ResolveAsync(treeId);
        Assert.That(resized, Is.Not.EqualTo(treeId), "precondition: the tree resolves to its resized copy");

        // A write the resized copy takes, which the shipper may already have
        // sent to a peer: the undo would discard it here only.
        await tree.SetAsync("after", Bytes("2"));

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => tree.UndoResizeAsync());

        Assert.Multiple(async () =>
        {
            Assert.That(refusal!.Message, Does.Contain("replicated"));
            Assert.That(await Registry.ResolveAsync(treeId), Is.EqualTo(resized),
                "a refused undo leaves the alias on the resized copy");
            Assert.That(await tree.IsResizeUndoPendingAsync(), Is.False, "the refused undo is withdrawn");
            Assert.That(Text(await tree.GetAsync("after")), Is.EqualTo("2"), "the resized copy's writes stay");
            Assert.That(Text(await tree.GetAsync("before")), Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task An_undo_before_the_swap_of_a_replicated_tree_still_unwinds()
    {
        var treeId = $"{ReplicatedPrefix}undo-before-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k", Bytes("1"));

        var hold = SliceGate.Arm(treeId);
        try
        {
            await tree.ResizeAsync(64, 64);
            await hold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await tree.UndoResizeAsync().WaitAsync(TimeSpan.FromSeconds(5)).ContinueWith(static _ => { });
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        await TestPoll.UntilAsync(
            async () => !await tree.IsResizeUndoPendingAsync() && await Registry.ResolveAsync(treeId) == treeId,
            "the pre-swap undo to unwind",
            timeout: TimeSpan.FromSeconds(30));
        Assert.That(Text(await tree.GetAsync("k")), Is.EqualTo("1"));
    }

    [Test]
    public async Task An_undo_after_the_swap_of_an_unreplicated_tree_still_runs()
    {
        var treeId = $"plain-undo-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k", Bytes("1"));
        await ResizeToCompletionAsync(tree);

        await tree.UndoResizeAsync();
        await TestPoll.UntilAsync(
            async () => !await tree.IsResizeUndoPendingAsync() && await Registry.ResolveAsync(treeId) == treeId,
            "the undo to finish",
            timeout: TimeSpan.FromSeconds(30));
        Assert.That(Text(await tree.GetAsync("k")), Is.EqualTo("1"));
    }

    private static async Task ResizeToCompletionAsync(ILattice tree)
    {
        await tree.ResizeAsync(64, 64);
        await TestPoll.UntilAsync(() => tree.IsResizeCompleteAsync(), "the resize to complete",
            timeout: TimeSpan.FromSeconds(30));
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string? Text(byte[]? value) => value is null ? null : Encoding.UTF8.GetString(value);

    /// <summary>
    /// The replication package's view of which trees replicate, reduced to a
    /// prefix rule: a tree is replicated when the context reports a merge mode
    /// for it.
    /// </summary>
    private sealed class PrefixReplicationContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => "test-cluster";

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            treeId.StartsWith(ReplicatedPrefix, StringComparison.Ordinal) ? LatticeMergeMode.LwwRegister : null;
    }

    /// <summary>
    /// Holds the resize's snapshot slice for an armed tree so an undo lands
    /// while the copy is still draining, before the swap.
    /// </summary>
    private sealed class SliceGate : IOutgoingGrainCallFilter
    {
        private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

        internal static Hold Arm(string treeId) => Holds.GetOrAdd(treeId, static _ => new Hold());

        internal static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.MethodName == nameof(ITreeSnapshotGrain.RunSnapshotSliceAsync)
                && Holds.TryGetValue(context.TargetId.Key.ToString()!, out var hold))
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<ILatticeReplicationContext, PrefixReplicationContext>();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<SliceGate>();
        }
    }
}
