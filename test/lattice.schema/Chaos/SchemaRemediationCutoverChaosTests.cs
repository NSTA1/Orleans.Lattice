using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree;

namespace Orleans.Lattice.Schema.Tests.Chaos;

/// <summary>
/// Atomic reads across a schema remediation cutover of a tree an online reshard has
/// re-mapped. A remediation builds a destination copy and swaps the logical tree's
/// alias onto it, carrying the destination's map across the swap (#4250); the
/// existing coverage of that carry reads the tree only after the cutover.
/// <para>
/// The remediation's contract is that the tree is write-quiesced while it builds
/// (<c>LatticeSchemaRemediationGrain</c>, "Concurrent-writes contract"), so atomic
/// batches are written - through <see cref="AtomicRoundProbe"/> - before and after
/// each remediation, and only the continuous readers run while it cuts over. The
/// transform is a passthrough, so the destination holds exactly the values the
/// source did: every poll during the cutover must see every key at the last
/// committed round, with no rollback allowed. Between cutovers the tree is resharded
/// through its alias under atomic load, so the second cutover replaces a copy whose
/// map a reshard rewrote after the first.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class SchemaRemediationCutoverChaosTests
{
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(90);

    private SchemaRemediationClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeRegistry Registry => Grains.GetLatticeRegistry();

    [Test]
    public async Task Readers_see_every_key_at_the_committed_round_through_remediation_cutovers_of_a_resharded_tree()
    {
        var treeId = $"remediate-chaos-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 4, MaxLeafKeys = 4 });
        var tree = Grains.GetGrain<ILattice>(treeId);
        var probe = new AtomicRoundProbe(tree, "remediate-tx");
        await probe.SeedAsync();
        probe.StartReaders();

        var log = new List<string>();
        var problems = new List<string>();
        var physicalIds = new List<string> { await Registry.ResolveAsync(treeId) };
        var shardCounts = new List<int>();

        await tree.ReshardAsync(7);
        log.Add((await probe.RunPhaseAsync("grow 4->7", TopologyDrivers.ReshardStep(Grains, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after grow"));

        await RemediateWithReadersOnlyAsync(probe, "first remediation cutover", treeId,
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }), log);
        problems.AddRange(await probe.VerifyQuiescedAsync("after the first cutover"));
        physicalIds.Add(await Registry.ResolveAsync(treeId));
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(Grains, treeId)).Count);

        await tree.ReshardAsync(3);
        log.Add((await probe.RunPhaseAsync("shrink 7->3 through the alias", TopologyDrivers.ReshardStep(Grains, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after shrink"));

        await RemediateWithReadersOnlyAsync(probe, "second remediation cutover", treeId,
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json(), LatticeSchemaRule.MaxLength(4096) }), log);
        problems.AddRange(await probe.VerifyQuiescedAsync("after the second cutover"));
        physicalIds.Add(await Registry.ResolveAsync(treeId));
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(Grains, treeId)).Count);

        await tree.ReshardAsync(5);
        log.Add((await probe.RunPhaseAsync("grow 3->5 after the second cutover", TopologyDrivers.ReshardStep(Grains, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        await probe.StopReadersAsync();
        problems.AddRange(await probe.VerifyQuiescedAsync("after the final grow"));
        shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(Grains, treeId)).Count);

        TestContext.Out.WriteLine(string.Join(Environment.NewLine, log));
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic read violation across a remediation cutover:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(physicalIds.Distinct().Count(), Is.EqualTo(3), "each remediation must cut the tree over onto a new copy");
            Assert.That(shardCounts, Is.EqualTo(new[] { 7, 3, 5 }),
                "each cutover must carry the map its copy was written under, and later reshards must re-map that copy");
            Assert.That(probe.UniformPolls, Is.GreaterThan(0));
        });
    }

    private async Task RemediateWithReadersOnlyAsync(
        AtomicRoundProbe probe, string phase, string treeId, LatticeSchemaPolicy policy, List<string> log)
    {
        probe.MarkPhase(phase);
        var started = DateTime.UtcNow;
        var report = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId)
            .StartAsync(LatticeValueTransform.Passthrough(), policy);
        log.Add($"{phase}: succeeded={report.Succeeded} in {(DateTime.UtcNow - started).TotalMilliseconds:F0} ms");
        Assert.That(report.Succeeded, Is.True, "precondition: the remediation cut over");
    }
}
