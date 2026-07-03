using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Proves the security-critical system-origin apply bypass (issue #982): the
/// receiver-side apply path runs under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so a replicated write
/// to a gated tree (for example the reserved auth policy tree) is applied as
/// receiver-side convergence and is never re-authorized as a user write - the
/// "caller" on the receiving cluster has no user identity. The tests observe the
/// ambient <see cref="LatticeAccessGateContext.IsSystemOrigin"/> flag at the exact
/// moment the applier drives the core apply seam.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_drives_the_apply_seam_under_a_system_origin_scope()
    {
        var (applier, _, apply, _) = CreateApplier();
        bool? systemOriginDuringApply = null;
        apply
            .When(x => x.ApplySetAsync(
                Arg.Any<string>(),
                Arg.Any<byte[]>(),
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<long>()))
            .Do(_ => systemOriginDuringApply = LatticeAccessGateContext.IsSystemOrigin);

        await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.That(systemOriginDuringApply, Is.True);
    }

    [Test]
    public async Task ApplyAsync_restores_the_ambient_system_origin_state_after_returning()
    {
        var (applier, _, _, _) = CreateApplier();
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "precondition");

        await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }

    [Test]
    public async Task ApplyBatchAsync_drives_every_apply_under_a_system_origin_scope()
    {
        var (applier, _, apply, _) = CreateApplier();
        var observed = new List<bool>();
        apply
            .When(x => x.ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>()))
            .Do(_ => observed.Add(LatticeAccessGateContext.IsSystemOrigin));

        await applier.ApplyBatchAsync(new[]
        {
            SetEntry("k1", Hlc(10)),
            SetEntry("k2", Hlc(11)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(observed, Has.Count.GreaterThanOrEqualTo(1));
            Assert.That(observed, Has.All.True);
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "restored after batch");
        });
    }
}
