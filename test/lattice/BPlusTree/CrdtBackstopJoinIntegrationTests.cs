using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4611 on a real cluster. A staged CRDT write rides a cross-tree atomic
/// batch and prepares on a key's leaf; its terminal is then held at the saga
/// coordinator. Writes below the key split that leaf, so the key's range moves to a
/// new sibling while the saga's bucket stays stranded on the donor, and an
/// acknowledged non-atomic CRDT write of the same key lands on the sibling. On
/// release, the saga's value reaches the sibling through the terminal's backstop,
/// which used to install the stage-time state last-writer-wins at a dominating
/// stamp and lose the acknowledged write. The backstop now joins the state into the
/// row, as the drain folds the delta.
/// <para>
/// The trees are declared CRDT through a merge-mode resolver, as the replication
/// package declares them: without one, the drain does not fold either, which is the
/// documented single-cluster caveat on <see cref="LatticeStagedCrdtWrite"/>.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtBackstopJoinIntegrationTests
{
    private const string OrSetPrefix = "orset-backstop-";
    private const string GCounterPrefix = "gcounter-backstop-";

    /// <summary>Sorts above every filler key, so a split of its leaf moves it to the sibling.</summary>
    private const string Key = "z-key";

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
        SagaTerminalHold.Release();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TearDown]
    public void ReleaseHold() => SagaTerminalHold.Release();

    private async Task<ILattice> CreateTreeAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        return _cluster.GrainFactory.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Commits <paramref name="staged"/> with its terminal held, runs
    /// <paramref name="whileHeld"/>, then releases the terminal and awaits the commit.
    /// </summary>
    private async Task CommitWithHeldTerminalAsync(
        string treeId, LatticeStagedCrdtWrite staged, Func<Task> whileHeld)
    {
        SagaTerminalHold.Arm();
        var commit = _cluster.GrainFactory.BeginAtomicWrite($"backstop-{Guid.NewGuid():N}")
            .ForTree(treeId).Set(staged)
            .CommitAsync();
        try
        {
            var reached = await Task.WhenAny(SagaTerminalHold.Reached, commit, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(reached, Is.SameAs(SagaTerminalHold.Reached),
                "precondition: the saga decided and its terminal broadcast is held");
            await whileHeld();
        }
        finally
        {
            SagaTerminalHold.Release();
        }

        Assert.That(await commit, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
    }

    /// <summary>Splits the key's leaf: fills it from below with CRDT writes until it divides.</summary>
    private static async Task SplitBelowKeyAsync(ILattice tree, Func<string, Task> write)
    {
        for (var i = 0; i < 12; i++)
            await write($"a-{i:D2}");
    }

    [Test]
    public async Task A_stranded_or_set_add_keeps_an_add_acknowledged_after_its_leaf_split()
    {
        var treeId = $"{OrSetPrefix}{Guid.NewGuid():N}";
        var tree = await CreateTreeAsync(treeId);
        var staged = await tree.OrSet(Key).StageAddAsync(Encoding.UTF8.GetBytes("x"), "r1");

        await CommitWithHeldTerminalAsync(treeId, staged, async () =>
        {
            await SplitBelowKeyAsync(tree, filler => tree.OrSet(filler).AddAsync(Encoding.UTF8.GetBytes("f"), "r3"));
            await tree.OrSet(Key).AddAsync(Encoding.UTF8.GetBytes("y"), "r2");
        });

        Assert.Multiple(async () =>
        {
            Assert.That(await tree.OrSet(Key).ContainsAsync(Encoding.UTF8.GetBytes("x")), Is.True, "the saga's committed add");
            Assert.That(await tree.OrSet(Key).ContainsAsync(Encoding.UTF8.GetBytes("y")), Is.True,
                "the add acknowledged while the saga's terminal was in flight must survive it");
        });
    }

    [Test]
    public async Task A_stranded_g_counter_increment_keeps_an_increment_acknowledged_after_its_leaf_split()
    {
        var treeId = $"{GCounterPrefix}{Guid.NewGuid():N}";
        var tree = await CreateTreeAsync(treeId);
        var staged = await tree.GCounter(Key).StageIncrementAsync("A", 1);

        await CommitWithHeldTerminalAsync(treeId, staged, async () =>
        {
            await SplitBelowKeyAsync(tree, filler => tree.GCounter(filler).IncrementAsync("C", 1));
            await tree.GCounter(Key).IncrementAsync("B", 1);
        });

        Assert.That(await tree.GCounter(Key).ValueAsync(), Is.EqualTo(2),
            "A's committed increment and B's acknowledged increment must both count");
    }

    /// <summary>Declares the test trees CRDT, as the replication package's resolver does.</summary>
    private sealed class PrefixMergeModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(OrSetPrefix, StringComparison.Ordinal) ? LatticeMergeMode.OrSet
            : treeId.StartsWith(GCounterPrefix, StringComparison.Ordinal) ? LatticeMergeMode.GCounter
            : null;
    }

    /// <summary>Holds the saga coordinator's terminal broadcast; every other terminal passes.</summary>
    private static class SagaTerminalHold
    {
        private static TaskCompletionSource _reached = NewSource();
        private static TaskCompletionSource _released = NewSource();
        private static int _armed;

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static Task Reached => _reached.Task;

        internal static void Arm()
        {
            _reached = NewSource();
            _released = NewSource();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.TrySetResult();
        }

        internal static Task WaitIfArmedAsync()
        {
            if (Volatile.Read(ref _armed) == 0)
                return Task.CompletedTask;
            _reached.TrySetResult();
            return _released.Task;
        }
    }

    private sealed class SagaTerminalHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(IShardRootGrain.AppendTxTerminalAsync)
                && context.SourceId is { } sourceId
                && sourceId.Type.ToString().Contains("atomicwrite", StringComparison.OrdinalIgnoreCase))
            {
                await SagaTerminalHold.WaitIfArmedAsync();
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, PrefixMergeModeResolver>();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<SagaTerminalHoldFilter>();
        }
    }
}
