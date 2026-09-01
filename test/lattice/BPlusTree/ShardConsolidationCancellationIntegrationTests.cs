using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for cancelling an online shard consolidation.
/// <para>
/// Cancellation is the one part of the fold whose contract is phase-dependent:
/// a request is honoured only while the routing map is still untouched
/// (pre-<see cref="ShardConsolidationPhase.Swap"/>), and past that point it is
/// recorded but deliberately not acted on, because abandoning a fold
/// mid-retirement would strand the donor. Testing that against a real cluster
/// means the test has to be certain which side of that line the fold was on -
/// the coordinator's own background phase timer is otherwise free to carry it
/// across between two client calls.
/// </para>
/// <para>
/// This fixture therefore runs on
/// <see cref="ConsolidationSlowPumpClusterFixture"/>, which drains one donor
/// leaf per background pass so a populated tree stays cancellable for tens of
/// seconds, and every assertion below is made against the phase the test
/// <em>observed</em> rather than the phase it assumed.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardConsolidationCancellationIntegrationTests
{
    private const int DonorShard = 3;
    private const int SurvivorShard = 2;

    /// <summary>
    /// Key count chosen so the donor shard owns a long leaf chain. At
    /// <see cref="ConsolidationSlowPumpClusterFixture.SmallMaxLeafKeys"/> keys
    /// per leaf across four shards this leaves the donor with roughly fifteen
    /// leaves, so the bounded drain needs roughly that many two-second pump
    /// ticks before the fold could reach <c>Swap</c>.
    /// </summary>
    private const int KeyCount = 240;

    private ConsolidationSlowPumpClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ConsolidationSlowPumpClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ITreeShardConsolidationGrain Consolidator(string treeId, int donorShardIndex)
        => _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{donorShardIndex}");

    private async Task<ShardMap> GetMapAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.GetShardMapAsync(treeId)
            ?? ShardMap.CreateDefault(
                LatticeConstants.DefaultVirtualShardCount,
                ConsolidationSlowPumpClusterFixture.TestShardCount);
    }

    private async Task<int> PhysicalShardCountAsync(string treeId)
        => (await GetMapAsync(treeId)).GetPhysicalShardIndices().Count;

    private static async Task<Dictionary<string, byte[]>> PopulateAsync(ILattice tree, string prefix, int count)
    {
        var expected = new Dictionary<string, byte[]>(count);
        for (var i = 0; i < count; i++)
        {
            var key = $"{prefix}-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"value-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }
        return expected;
    }

    private static async Task AssertAllReadableAsync(
        ILattice tree, Dictionary<string, byte[]> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' unreachable {when}.");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Key '{key}' has the wrong value {when}.");
        }
    }

    /// <summary>
    /// Drives a fold to a terminal state, then asserts it got there. The pump
    /// may advance it concurrently; the grain is non-reentrant, so an explicit
    /// pass and a pump tick can never interleave mid-phase.
    /// </summary>
    private async Task RunFoldToIdleAsync(ITreeShardConsolidationGrain coordinator)
    {
        for (var i = 0; i < 256 && !await coordinator.IsIdleAsync(); i++)
            await coordinator.RunConsolidationPassAsync();

        Assert.That(await coordinator.IsIdleAsync(), Is.True, "The fold must reach a terminal state.");
    }

    /// <summary>
    /// Asserts that <paramref name="observed"/> - a progress snapshot taken
    /// immediately after a cancel request - is consistent with the fold having
    /// been pre-<c>Swap</c> when the request was decided, and returns nothing
    /// otherwise having failed with a diagnostic.
    /// <para>
    /// Only two snapshots are consistent with that, and both imply the request
    /// was accepted: the fold is still in flight and still pre-<c>Swap</c>
    /// (the pump has not acted on the request yet), or it is already terminally
    /// <c>Cancelled</c> (the pump acted between the two calls). A fold that has
    /// advanced to <c>Swap</c> or completed is the one state that would mean the
    /// request was legitimately refused, and it means this test never reached
    /// the scenario it exists to cover.
    /// </para>
    /// <para>
    /// This is a reachability check on the scenario, not a softening of the
    /// contract: the caller asserts acceptance unconditionally afterwards.
    /// </para>
    /// </summary>
    private static void AssertStillCancellableWhenRequested(ShardConsolidationProgress observed)
    {
        var stillPreSwap = observed.InProgress
            && (int)observed.Phase < (int)ShardConsolidationPhase.Swap;
        var alreadyAbandoned = !observed.InProgress && observed.Cancelled;

        Assert.That(stillPreSwap || alreadyAbandoned, Is.True,
            "The fold advanced past its point of no return before the cancel request was decided, so this "
            + "run never exercised the pre-Swap cancellation contract. The slow-pump fixture drains one "
            + $"donor leaf per two-second pass specifically to prevent that. Observed: InProgress="
            + $"{observed.InProgress}, Phase={observed.Phase}, Cancelled={observed.Cancelled}, "
            + $"Complete={observed.Complete}, LeavesScanned={observed.LeavesScanned}.");
    }

    [Test]
    public async Task A_cancelled_fold_leaves_the_tree_readable_writable_and_foldable_again()
    {
        var treeId = $"cons-cancel-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "ck", KeyCount);

        var coordinator = Consolidator(treeId, DonorShard);
        await coordinator.StartAsync(SurvivorShard);

        // Request the cancel, then observe. Phases only ever advance while a
        // fold is in flight, so a snapshot showing the fold still pre-Swap
        // proves it was pre-Swap when CancelAsync made its decision. That makes
        // the acceptance assertion below an implication rather than a bet on
        // which of two client calls the background pump landed between.
        var accepted = await coordinator.CancelAsync();
        var observed = await coordinator.GetProgressAsync();

        AssertStillCancellableWhenRequested(observed);
        Assert.That(accepted, Is.True,
            "A cancel requested while the routing map is still untouched must be accepted.");

        await RunFoldToIdleAsync(coordinator);

        var progress = await coordinator.GetProgressAsync();
        Assert.That(progress.Cancelled, Is.True);
        Assert.That(progress.Complete, Is.False);
        Assert.That(await PhysicalShardCountAsync(treeId),
            Is.EqualTo(ConsolidationSlowPumpClusterFixture.TestShardCount),
            "An abandoned fold must leave the tree's physical topology exactly as it was.");

        await AssertAllReadableAsync(tree, expected, "after the fold was abandoned");
        await tree.SetAsync("post-cancel", Encoding.UTF8.GetBytes("ok"));
        Assert.That(await tree.GetAsync("post-cancel"), Is.Not.Null);

        // The abandoned fold must not have poisoned the pair.
        var refold = Consolidator(treeId, DonorShard);
        await refold.StartAsync(SurvivorShard);
        await RunFoldToIdleAsync(refold);

        Assert.That(await PhysicalShardCountAsync(treeId),
            Is.EqualTo(ConsolidationSlowPumpClusterFixture.TestShardCount - 1));
        await AssertAllReadableAsync(tree, expected, "after re-folding the previously abandoned pair");
    }

    [Test]
    public async Task A_cancel_request_is_recorded_even_before_the_pump_acts_on_it()
    {
        var treeId = $"cons-cancel-flag-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "cf", KeyCount);

        var coordinator = Consolidator(treeId, DonorShard);
        await coordinator.StartAsync(SurvivorShard);

        var accepted = await coordinator.CancelAsync();
        var observed = await coordinator.GetProgressAsync();

        AssertStillCancellableWhenRequested(observed);
        Assert.That(accepted, Is.True);

        // Whether or not the pump has acted yet, a driver polling progress must
        // be able to see that its request was received - otherwise it cannot
        // distinguish "not yet honoured" from "never arrived" and would re-issue.
        Assert.That(observed.CancelRequested || observed.Cancelled, Is.True,
            "A recorded cancel must be observable through GetProgressAsync.");

        await RunFoldToIdleAsync(coordinator);
        Assert.That((await coordinator.GetProgressAsync()).Cancelled, Is.True);
    }

    [Test]
    public async Task Cancelling_a_fold_that_never_started_is_refused()
    {
        var treeId = $"cons-cancel-none-{Guid.NewGuid():N}";
        await _fixture.CreateTreeAsync(treeId);

        var coordinator = Consolidator(treeId, DonorShard);

        // Deterministic: no fold has ever been started on this coordinator, so
        // there is no pump and nothing to abandon.
        Assert.That(await coordinator.CancelAsync(), Is.False);
        Assert.That(await coordinator.IsIdleAsync(), Is.True);
    }
}
