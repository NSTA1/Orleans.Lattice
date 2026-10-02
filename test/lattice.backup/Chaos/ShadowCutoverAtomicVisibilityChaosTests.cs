using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree;

namespace Orleans.Lattice.Backup.Tests.Chaos;

/// <summary>
/// Atomic read and write guarantees across shadow-cutover restores and their
/// reverts, on trees whose shard map an online reshard has rewritten. A cutover
/// swaps the logical tree's alias onto a freshly built shadow and must carry the
/// shadow's map with it (#4250); a revert swaps back and must carry the replaced map
/// back; and a split or fold still in flight when either lands must be abandoned
/// rather than commit its slot diff to the wrong copy's map (#4310). The existing
/// cutover chaos suites (<see cref="ShadowCutoverRoutingSelfHealChaosTests"/>,
/// <see cref="CrossTreeAtomicWriteAcrossCutoverChaosTests"/>) run on identity-mapped
/// trees and check routing self-heal and liveness, not whether a batch survives the
/// swap whole.
/// <para>
/// A chain of <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// rounds runs throughout against a 16-key universe while continuous readers poll it
/// (see <see cref="AtomicRoundProbe"/>). A restore rolls the tree back to the
/// backup's point in time and a revert rolls it back to the replaced copy, so while
/// one runs a poll may observe an older round than the last committed - but never a
/// mix of rounds, and never a key missing. Once it settles, every later round must
/// stick, and the tree must count and scan exactly the universe.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class ShadowCutoverAtomicVisibilityChaosTests
{
    private const int InitialShards = 4;

    /// <summary>
    /// The shard count a restore shadow is registered with: the library default,
    /// because the shadow's registry entry pins none.
    /// </summary>
    private const int ShadowShards = LatticeConstants.DefaultShardCount;
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(90);

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private IGrainFactory GrainFactory => _fixture.GrainFactory;

    private ILatticeRegistry Registry => GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task Cutovers_and_a_revert_of_a_resharded_tree_never_tear_an_atomic_batch()
    {
        var treeId = $"cutover-atomic-{Guid.NewGuid():N}";
        var tree = await CreateTreeAsync(treeId);
        var probe = new AtomicRoundProbe(tree, "cut-tx", isToleratedWriteFault: IsRolledBackSaga);
        await probe.SeedAsync();
        probe.StartReaders();

        var log = new List<string>();
        var problems = new List<string>();

        await tree.ReshardAsync(8);
        log.Add((await probe.RunPhaseAsync("grow 4->8", TopologyDrivers.ReshardStep(GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        await tree.ReshardAsync(5);
        log.Add((await probe.RunPhaseAsync("shrink 8->5", TopologyDrivers.ReshardStep(GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after reshards"));

        var backupId = await CaptureAsync(treeId);
        await probe.RunPhaseAsync("diverge from the backup", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5);

        var original = await Registry.ResolveAsync(treeId);
        var first = await CutoverUnderLoadAsync(probe, treeId, backupId, "first cutover", log, problems);
        var afterFirst = await Registry.ResolveAsync(treeId);

        await RunUnderRollbackAsync(probe, "revert", () => _fixture.Restore.RevertRestoreAsync(first), log, problems);
        var afterRevert = await Registry.ResolveAsync(treeId);

        // Reshard the reverted tree, then cut it over again: the carried-back map must
        // still describe the copy the alias points at.
        await tree.ReshardAsync(7);
        log.Add((await probe.RunPhaseAsync("grow 5->7 after revert", TopologyDrivers.ReshardStep(GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync("after regrow"));
        var secondBackup = await CaptureAsync(treeId);
        await probe.RunPhaseAsync("diverge again", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5);
        await CutoverUnderLoadAsync(probe, treeId, secondBackup, "second cutover", log, problems);

        await probe.StopReadersAsync();
        TestContext.Out.WriteLine(string.Join(Environment.NewLine, log));
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a shadow cutover or revert:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(afterFirst, Is.Not.EqualTo(original), "precondition: the first cutover swapped the alias");
            Assert.That(afterRevert, Is.EqualTo(original), "the revert must return the alias to the replaced copy");
            Assert.That(probe.UniformPolls, Is.GreaterThan(0));
        });
    }

    // The tree starts at the shard count a restore shadow is registered with, so once
    // the cutover carries the shadow's map onto it the reshard still has the same
    // distance to travel - four splits or four folds - on the new copy.
    [TestCase(ShadowShards + 4, TestName = "A_cutover_landing_mid_grow_never_tears_an_atomic_batch_and_the_grow_completes")]
    [TestCase(ShadowShards - 4, TestName = "A_cutover_landing_mid_shrink_never_tears_an_atomic_batch_and_the_shrink_completes")]
    public async Task A_cutover_landing_mid_reshard_never_tears_an_atomic_batch(int target)
    {
        var treeId = $"cutover-mid-reshard-{Guid.NewGuid():N}";
        var tree = await CreateTreeAsync(treeId, ShadowShards);
        var probe = new AtomicRoundProbe(tree, "mid-tx", isToleratedWriteFault: IsRolledBackSaga);
        await probe.SeedAsync();
        var backupId = await CaptureAsync(treeId);
        probe.StartReaders();
        await probe.RunPhaseAsync("diverge from the backup", _ => Task.FromResult(true), PhaseBudget, tailRounds: 3);

        var reshardStep = TopologyDrivers.ReshardStep(GrainFactory, treeId);
        var reshard = GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var original = await Registry.ResolveAsync(treeId);
        var cutoverLanded = false;
        var migrationsInFlightAtCutover = 0;

        await tree.ReshardAsync(target);
        AtomicRoundProbe.PhaseReport phase;
        using (probe.OpenRollbackWindow())
        {
            var steps = 0;
            phase = await probe.RunPhaseAsync($"reshard {ShadowShards}->{target} with a cutover mid-flight", async ct =>
            {
                // Step 1 lets the coordinator start its first migrations without
                // driving them; step 2 restores the tree underneath them; every later
                // step drives the reshard to completion on the cut-over copy's map.
                switch (++steps)
                {
                    case 1:
                        await reshard.RunReshardPassAsync();
                        return false;
                    case 2:
                        migrationsInFlightAtCutover = await CountMigrationsInFlightAsync(treeId);
                        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
                            backupId, treeId, scope: null, mode: LatticeRestoreMode.ShadowCutover));
                        cutoverLanded = true;
                        return false;
                    default:
                        return await reshardStep(ct);
                }
            }, PhaseBudget, isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
        }

        var settle = await probe.RunPhaseAsync("settle", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5);
        await probe.StopReadersAsync();
        var problems = await probe.VerifyQuiescedAsync("after the reshard and cutover");
        var live = await TopologyDrivers.PhysicalShardsAsync(GrainFactory, treeId);
        var idle = await reshard.IsIdleAsync();
        var physical = await Registry.ResolveAsync(treeId);
        TestContext.Out.WriteLine($"{phase}{Environment.NewLine}{settle}{Environment.NewLine}migrationsInFlightAtCutover={migrationsInFlightAtCutover} live=[{string.Join(",", live)}]");
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a cutover that landed mid-reshard:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(cutoverLanded, Is.True, "precondition: the cutover ran while the reshard was in flight");
            Assert.That(migrationsInFlightAtCutover, Is.GreaterThan(0),
                "precondition: at least one split or fold was in flight when the cutover landed");
            Assert.That(physical, Is.Not.EqualTo(original), "precondition: the cutover swapped the alias");
            Assert.That(phase.Completed, Is.True, "the reshard must complete after a cutover lands under it");
            Assert.That(idle, Is.True);
            Assert.That(live, Has.Count.EqualTo(target), "the reshard must reach its target on the cut-over copy's map");
        });
    }

    private async Task<ILattice> CreateTreeAsync(string treeId, int shardCount = InitialShards)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = shardCount, MaxLeafKeys = 4 });
        return GrainFactory.GetGrain<ILattice>(treeId);
    }

    private async Task<string> CaptureAsync(string treeId)
    {
        var backup = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest(
            $"chaos-{Guid.NewGuid():N}", BackupScopeSelector.WholeTree(treeId)));
        return backup.BackupId;
    }

    private async Task<LatticeRestoreResult> CutoverUnderLoadAsync(
        AtomicRoundProbe probe, string treeId, string backupId, string phase, List<string> log, List<string> problems)
    {
        LatticeRestoreResult? result = null;
        await RunUnderRollbackAsync(probe, phase, async () =>
        {
            result = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
                backupId, treeId, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        }, log, problems);
        return result!;
    }

    private static async Task RunUnderRollbackAsync(
        AtomicRoundProbe probe, string phase, Func<Task> action, List<string> log, List<string> problems)
    {
        using (probe.OpenRollbackWindow())
        {
            var ran = 0;
            log.Add((await probe.RunPhaseAsync(phase, async _ =>
            {
                if (Interlocked.Exchange(ref ran, 1) == 0) await action();
                return true;
            }, PhaseBudget)).ToString());
        }

        log.Add((await probe.RunPhaseAsync($"settle after {phase}", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5)).ToString());
        problems.AddRange(await probe.VerifyQuiescedAsync($"after {phase}"));
    }

    private async Task<int> CountMigrationsInFlightAsync(string treeId)
    {
        var inFlight = 0;
        for (var index = 0; index < ShadowShards + 8; index++)
        {
            if (!await GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}").IsIdleAsync()) inFlight++;
            if (!await GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{index}").IsIdleAsync()) inFlight++;
        }

        return inFlight;
    }

    /// <summary>
    /// A saga that cannot complete across the swap rolls back as a unit and reports
    /// it; the batch is then absent everywhere, which the readers still verify.
    /// </summary>
    private static bool IsRolledBackSaga(Exception ex) =>
        ex is InvalidOperationException && ex.Message.Contains("rolled back", StringComparison.Ordinal);
}
