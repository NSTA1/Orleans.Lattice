using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TreeAdmin.Tests.Chaos;

/// <summary>
/// Atomic read and write guarantees across the operator's explicit alias swap,
/// <see cref="LatticeTreeAdmin.SetTreeAliasAsync"/>, onto trees an online reshard has
/// re-mapped, while a chain of atomic batches is written through the logical tree and
/// continuous readers poll it. The swap must carry the target's map onto the logical
/// tree (#4263), every routing activation the readers and writers warmed against the
/// previous copy must move onto the target, and a reshard driven through the alias
/// afterwards must re-map the target rather than the copy the alias left.
/// <para>
/// Each target is prepared, quiesced, holding the whole universe at one round, so a
/// swap rolls the logical tree back to that round - the readers' lower bound is
/// relaxed for the swap (see <see cref="AtomicRoundProbe.OpenRollbackWindow"/>), never
/// their uniformity. Once the swap settles, every round written afterwards must stick.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class ExplicitAliasSwapChaosTests
{
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(90);

    private TestCluster _cluster = null!;
    private LatticeTreeAdmin _admin = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _admin = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            _cluster.Client,
            new TreeAdminAccessAuthorizer(new AllowGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IGrainFactory GrainFactory => _cluster.Client;

    private ILatticeRegistry Registry => GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task Atomic_batches_stay_whole_through_explicit_alias_swaps_onto_resharded_trees()
    {
        var logicalId = $"alias-logical-{Guid.NewGuid():N}";
        var logical = await CreateTreeAsync(logicalId, 4);
        var probe = new AtomicRoundProbe(logical, "alias-tx");
        await probe.SeedAsync();
        probe.StartReaders();

        var log = new List<string>();
        var problems = new List<string>();
        var shardCounts = new List<int>();

        await logical.ReshardAsync(6);
        log.Add((await probe.RunPhaseAsync("grow the logical tree 4->6",
            TopologyDrivers.ReshardStep(GrainFactory, logicalId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after grow"));

        var firstTarget = await PrepareTargetAsync(probe, shardCount: 4, reshardTo: 7);
        await SwapUnderLoadAsync(probe, logicalId, firstTarget, log, problems);
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(GrainFactory, logicalId)).Count);
        var afterFirst = await Registry.ResolveAsync(logicalId);

        // A shrink driven through the alias must fold the target's shards.
        await logical.ReshardAsync(4);
        log.Add((await probe.RunPhaseAsync("shrink 7->4 through the alias",
            TopologyDrivers.ReshardStep(GrainFactory, logicalId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after shrink through the alias"));
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(GrainFactory, logicalId)).Count);

        var secondTarget = await PrepareTargetAsync(probe, shardCount: 5, reshardTo: 3);
        await SwapUnderLoadAsync(probe, logicalId, secondTarget, log, problems);
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(GrainFactory, logicalId)).Count);
        var afterSecond = await Registry.ResolveAsync(logicalId);

        await probe.StopReadersAsync();
        TestContext.Out.WriteLine(string.Join(Environment.NewLine, log));
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across an explicit alias swap:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(afterFirst, Is.EqualTo(firstTarget));
            Assert.That(afterSecond, Is.EqualTo(secondTarget));
            Assert.That(shardCounts, Is.EqualTo(new[] { 7, 4, 3 }),
                "the logical tree must route by each target's map, and a reshard through the alias must re-map the target");
        });
    }

    /// <summary>
    /// Creates a target tree holding the whole universe at the probe's last committed
    /// round, then reshards it to <paramref name="reshardTo"/> shards so its map
    /// differs from the logical tree's.
    /// </summary>
    private async Task<string> PrepareTargetAsync(AtomicRoundProbe probe, int shardCount, int reshardTo)
    {
        var targetId = $"alias-target-{Guid.NewGuid():N}";
        var target = await CreateTreeAsync(targetId, shardCount);
        var round = probe.CommittedRound;
        await target.SetManyAtomicAsync(
            probe.Keys.Select((key, i) => new KeyValuePair<string, byte[]>(key, AtomicRoundProbe.Value(round, i))).ToList());

        await target.ReshardAsync(reshardTo);
        var step = TopologyDrivers.ReshardStep(GrainFactory, targetId);
        using var budget = new CancellationTokenSource(PhaseBudget);
        while (!await step(budget.Token))
            await Task.Delay(50, budget.Token);

        Assert.That((await TopologyDrivers.PhysicalShardsAsync(GrainFactory, targetId)).Count, Is.EqualTo(reshardTo),
            "precondition: the target was re-mapped");
        return targetId;
    }

    private async Task SwapUnderLoadAsync(
        AtomicRoundProbe probe, string logicalId, string targetId, List<string> log, List<string> problems)
    {
        using (probe.OpenRollbackWindow())
        {
            var swapped = 0;
            log.Add((await probe.RunPhaseAsync($"alias swap onto {targetId}", async _ =>
            {
                if (Interlocked.Exchange(ref swapped, 1) == 0) await _admin.SetTreeAliasAsync(logicalId, targetId);
                return true;
            }, PhaseBudget)).ToString());
        }

        log.Add((await probe.RunPhaseAsync($"settle after swap onto {targetId}", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync($"after swap onto {targetId}"));
    }

    private async Task<ILattice> CreateTreeAsync(string treeId, int shardCount)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = shardCount, MaxLeafKeys = 4 });
        return GrainFactory.GetGrain<ILattice>(treeId);
    }

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
