using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>Issue #4775: a split must not publish a sibling before its birth row exists.</summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class SplitLeafBirthPublicationIntegrationTests
{
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
        BirthHold.Release();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Scan_during_split_before_sibling_birth_returns_all_donor_rows(bool hasSuccessor)
    {
        var treeId = $"birth-publication-{Guid.NewGuid():N}";
        await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var keys = new List<string>();
        var seedCount = hasSuccessor ? 5 : 4;
        for (var i = 0; i < seedCount; i++)
        {
            var key = $"k{i:D2}";
            await tree.SetAsync(key, [1]);
            keys.Add(key);
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var donorId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var donor = _cluster.Client.GetGrain<IBPlusLeafGrain>(donorId);
        var oldNext = await donor.GetNextSiblingAsync();
        Assert.That(oldNext.HasValue, Is.EqualTo(hasSuccessor), "control: exercise both chain shapes");

        // The head holds two keys after its first split. Bring it to capacity.
        if (hasSuccessor)
        {
            foreach (var key in new[] { "!00", "!01" })
            {
                await tree.SetAsync(key, [1]);
                keys.Add(key);
            }
        }

        BirthHold.Arm(treeId);
        keys.Add("!trigger");
        // Dispatch to the actual donor, leaving the non-reentrant ILattice
        // worker free for the concurrent scan.
        var write = donor.SetAsync("!trigger", [1]);
        try
        {
            var siblingId = await BirthHold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.That(write.IsCompleted, Is.False, "control: the division is suspended before birth");
            Assert.That(await _cluster.Client.GetGrain<IBPlusLeafGrain>(siblingId).GetTreeIdAsync(), Is.Null,
                "control: the sibling has not been initialized");

            var scanned = new List<string>();
            using var scanTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await foreach (var entry in tree.ScanEntriesAsync().WithCancellation(scanTimeout.Token))
            {
                scanned.Add(entry.Key);
            }

            Assert.That(scanned, Is.EquivalentTo(keys),
                "an unborn sibling must not be exposed to a scan; its rows are still on the donor");
            Assert.That(await donor.GetNextSiblingAsync(), Is.EqualTo(oldNext));
            if (oldNext is { } next)
            {
                Assert.That(await _cluster.Client.GetGrain<IBPlusLeafGrain>(next).GetPrevSiblingAsync(),
                    Is.EqualTo(donorId), "the reverse chain must not expose the unborn sibling either");
            }
        }
        finally
        {
            BirthHold.Release();
            await write.WaitAsync(TimeSpan.FromSeconds(20));
        }

        Assert.That(await donor.GetNextSiblingAsync(), Is.EqualTo(BirthHold.Entered.Task.Result));
        var after = new List<string>();
        await foreach (var entry in tree.ScanEntriesAsync())
            after.Add(entry.Key);
        Assert.That(after, Is.EquivalentTo(keys), "the completed division preserves every row");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Split_birth_failure_keeps_the_chain_readable_and_reactivation_resumes_the_intent(bool afterBirth)
    {
        var treeId = $"birth-recovery-{Guid.NewGuid():N}";
        await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 4; i++)
            await tree.SetAsync($"k{i}", [1]);
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var donorId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var donor = _cluster.Client.GetGrain<IBPlusLeafGrain>(donorId);

        BirthHold.Arm(treeId, failAfterBirth: afterBirth, fail: true);
        var write = donor.SetAsync("k4", [1]);
        GrainId siblingId;
        try
        {
            siblingId = await BirthHold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(20));
            BirthHold.Release();
            var failure = Assert.ThrowsAsync<InvalidOperationException>(async () => await write);
            Assert.That(failure!.Message, Does.Contain("injected birth failure"));
        }
        finally
        {
            BirthHold.Release();
        }

        Assert.That(await donor.GetNextSiblingAsync(), Is.Null, "a failed birth does not publish a chain link");
        var before = new List<string>();
        await foreach (var entry in tree.ScanEntriesAsync())
            before.Add(entry.Key);
        Assert.That(before, Is.EquivalentTo(new[] { "k0", "k1", "k2", "k3", "k4" }));

        await donor.ForceDeactivateAsync();
        await Task.Delay(200);
        // A write drives recovery through the same durable intent after reload.
        await donor.SetAsync("k0", [2]);
        Assert.That(await donor.GetNextSiblingAsync(), Is.EqualTo(siblingId),
            "recovery must reuse the persisted identity, not create an unrelated sibling");
        var after = new List<string>();
        await foreach (var entry in tree.ScanEntriesAsync())
            after.Add(entry.Key);
        Assert.That(after, Is.EquivalentTo(before));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, BirthHoldFilter>();
        }
    }

    private static class BirthHold
    {
        internal static string? TreeId { get; private set; }
        internal static TaskCompletionSource<GrainId> Entered { get; private set; } = NewEntered();
        internal static TaskCompletionSource Resume { get; private set; } = NewResume();
        internal static bool Fail { get; private set; }
        internal static bool FailAfterBirth { get; private set; }

        private static TaskCompletionSource<GrainId> NewEntered() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private static TaskCompletionSource NewResume() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static void Arm(string treeId, bool failAfterBirth = false, bool fail = false)
        {
            Entered = NewEntered();
            Resume = NewResume();
            TreeId = treeId;
            Fail = fail;
            FailAfterBirth = failAfterBirth;
        }

        internal static void Release()
        {
            TreeId = null;
            Resume.TrySetResult();
        }
    }

    private sealed class BirthHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod.Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync)
                && context.Request.GetArgument(0) is SiblingInitialization init
                && init.TreeId == BirthHold.TreeId)
            {
                var fail = BirthHold.Fail;
                var failAfterBirth = BirthHold.FailAfterBirth;
                BirthHold.Entered.TrySetResult(context.TargetId);
                await BirthHold.Resume.Task;
                if (fail)
                {
                    if (failAfterBirth)
                        await context.Invoke();
                    throw new InvalidOperationException("injected birth failure");
                }
            }

            await context.Invoke();
        }
    }
}
