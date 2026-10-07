using System.Collections.Concurrent;
using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4474, end to end on real grains: a saga bound to the resized copy of a
/// tree, whose resize is undone while its terminal broadcast is in flight, must
/// not land part of its batch on the old copy the undo makes the tree again.
/// The undo discards every write the resized copy accepted after the swap, so the
/// saga's batch is discarded with that copy, whole.
/// <para>
/// The interleaving is pinned with an outgoing-call filter that holds the saga's
/// terminal for one shard of the resized copy until the undo has finished.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ResizeUndoBoundSagaTerminalIntegrationTests
{
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
        TerminalGate.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_saga_bound_to_the_resized_copy_is_discarded_whole_when_the_resize_is_undone_mid_broadcast()
    {
        var treeId = $"undo-bound-saga-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var (k1, k2) = await TwoKeysOnDistinctShardsAsync(tree);
        await tree.SetAsync(k1, Bytes("pre"));
        await tree.SetAsync(k2, Bytes("pre"));

        await tree.ResizeAsync(64, 64);
        await TestPoll.UntilAsync(() => tree.IsResizeCompleteAsync(), "the resize to complete",
            timeout: TimeSpan.FromSeconds(30));

        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resized = await registry.ResolveAsync(treeId);
        Assert.That(resized, Is.Not.EqualTo(treeId), "precondition: the tree resolves to its resized copy");
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        var heldShard = routing.Map.Resolve(k2);
        Assert.That(routing.Map.Resolve(k1), Is.Not.EqualTo(heldShard), "precondition: the keys sit on distinct shards");

        var hold = TerminalGate.Arm($"{resized}/{heldShard}");
        Task saga;
        try
        {
            saga = tree.SetManyAtomicAsync(
            [
                new KeyValuePair<string, byte[]>(k1, Bytes("saga")),
                new KeyValuePair<string, byte[]>(k2, Bytes("saga")),
            ]);
            await hold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // The saga has decided and its terminal for k2's shard of the resized
            // copy is held. Undo the resize: the alias moves back to the old copy
            // and the resized copy, with every write it took, is discarded.
            await tree.UndoResizeAsync();
            await TestPoll.UntilAsync(
                async () => await registry.ResolveAsync(treeId) == treeId && !await tree.IsResizeUndoPendingAsync(),
                "the undo to finish",
                timeout: TimeSpan.FromSeconds(30));
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        // Before the fix the held terminal was refused by the discarded copy as a
        // deleted tree: the caller got that refusal for a decided batch and the
        // saga retried it on every keepalive tick, never completing.
        Assert.DoesNotThrowAsync(() => saga.WaitAsync(TimeSpan.FromSeconds(90)),
            "a terminal the discarded copy refuses counts as delivered, so the saga completes");

        var v1 = Text(await tree.GetAsync(k1));
        var v2 = Text(await tree.GetAsync(k2));
        var sent = hold.Sent.ToArray();
        TestContext.Out.WriteLine($"after undo: {k1}={v1} {k2}={v2}; terminals sent: {string.Join(", ", sent)}");

        Assert.Multiple(() =>
        {
            Assert.That((v1, v2), Is.EqualTo(("pre", "pre")),
                "the undo discards the resized copy's batch whole; none of it may land on the old copy");
            Assert.That(sent, Is.Not.Empty, "precondition: the gate observed the saga's terminals");
            Assert.That(sent, Has.All.StartsWith($"{resized}/"),
                "no terminal of a saga bound to the discarded copy is re-sent to the old copy");
        });
    }

    private static async Task<(string, string)> TwoKeysOnDistinctShardsAsync(ILattice tree)
    {
        var map = (await tree.GetRoutingAsync(forceRefresh: true)).Map;
        const string first = "key-000";
        for (var i = 1; i < 256; i++)
        {
            var candidate = $"key-{i:D3}";
            if (map.Resolve(candidate) != map.Resolve(first)) return (first, candidate);
        }

        throw new InvalidOperationException("no two keys on distinct shards");
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string? Text(byte[]? value) => value is null ? null : Encoding.UTF8.GetString(value);

    /// <summary>
    /// Holds a saga terminal addressed to one armed shard until the test releases
    /// it, and records the target of every saga terminal sent.
    /// </summary>
    private sealed class TerminalGate : IOutgoingGrainCallFilter
    {
        private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

        internal static Hold Arm(string shardKey) => Holds.GetOrAdd(shardKey, static _ => new Hold());

        internal static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.MethodName == nameof(IShardRootGrain.AppendTxTerminalAsync))
            {
                var key = context.TargetId.Key.ToString()!;
                foreach (var hold in Holds.Values)
                    hold.Sent.Enqueue(key);

                if (Holds.TryGetValue(key, out var held) && !held.Release.Task.IsCompleted)
                {
                    held.Entered.TrySetResult();
                    await held.Release.Task;
                }
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ConcurrentQueue<string> Sent { get; } = new();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<TerminalGate>();
        }
    }
}
