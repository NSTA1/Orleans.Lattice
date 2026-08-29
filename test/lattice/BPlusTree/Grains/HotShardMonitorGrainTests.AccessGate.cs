using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the system-origin scope the sampling pass must hold while it polls
/// the four bulk-maintenance status verbs on <see cref="ILattice"/>.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    // Those four verbs are access-gated (LatticeOperation.Read). The timer-driven
    // sampling pass runs with no caller identity, so on a deny-by-default tree the
    // gate fails closed and denies every poll. OnTimerTickAsync catches and logs
    // every exception as a routine warning, so such a denial would be swallowed and
    // auto-split would be silently disabled for the tree: a security fix quietly
    // breaking an availability feature. These tests fail if the EnterSystemOrigin
    // scope around the polls is ever removed.

    [Test]
    public async Task RunSamplingPass_polls_the_gated_status_verbs_under_system_origin()
    {
        var (grain, _, lattice, _, _, _, _) = CreateGrain();

        var observed = new List<bool>();
        lattice.IsResizeCompleteAsync().Returns(_ =>
        {
            observed.Add(LatticeAccessGateContext.IsSystemOrigin);
            return Task.FromResult(true);
        });
        lattice.IsReshardCompleteAsync().Returns(_ =>
        {
            observed.Add(LatticeAccessGateContext.IsSystemOrigin);
            return Task.FromResult(true);
        });
        lattice.IsMergeCompleteAsync().Returns(_ =>
        {
            observed.Add(LatticeAccessGateContext.IsSystemOrigin);
            return Task.FromResult(true);
        });
        lattice.IsSnapshotCompleteAsync().Returns(_ =>
        {
            observed.Add(LatticeAccessGateContext.IsSystemOrigin);
            return Task.FromResult(true);
        });

        await grain.RunSamplingPassAsync();

        Assert.That(observed, Has.Count.EqualTo(4), "all four status verbs should have been polled");
        Assert.That(observed, Is.All.True, "every status poll must carry the system-origin scope");
    }

    [Test]
    public async Task RunSamplingPass_restores_the_ambient_origin_after_polling()
    {
        var (grain, _, _, _, _, _, _) = CreateGrain();

        await grain.RunSamplingPassAsync();

        Assert.That(
            LatticeAccessGateContext.IsSystemOrigin,
            Is.False,
            "the system-origin scope must not leak past the status polls");
    }
}
