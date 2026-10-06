using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The bootstrap drop floor a receiver tree's high-water-mark grain holds
/// (issue #4549): installed from a bootstrap export, read with the origin's
/// high-water mark in one call, durable, and cleared with the tree's applied
/// identities whenever its contents are replaced.
/// </summary>
public partial class ReplicationHighWaterMarkGrainTests
{
    private static Dictionary<string, HybridLogicalClock> Watermarks(params (string Origin, HybridLogicalClock Clock)[] entries) =>
        entries.ToDictionary(e => e.Origin, e => e.Clock, StringComparer.Ordinal);

    private static Dictionary<string, HybridLogicalClock[]> Held(params (string Origin, HybridLogicalClock[] Clocks)[] entries) =>
        entries.ToDictionary(e => e.Origin, e => e.Clocks, StringComparer.Ordinal);

    [Test]
    public async Task GetAdmissionAsync_reports_no_floor_on_a_fresh_tree()
    {
        IReplicationHighWaterMarkGrain grain = CreateGrain();

        var admission = await grain.GetAdmissionAsync(OriginA);

        Assert.Multiple(() =>
        {
            Assert.That(admission.BootstrapFloor, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(admission.HeldBelowFloor, Is.Empty);
            Assert.That(admission.Drops(Hlc(1)), Is.False, "no floor drops nothing");
        });
    }

    [Test]
    public async Task GetAdmissionAsync_carries_the_high_water_mark()
    {
        IReplicationHighWaterMarkGrain grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(7));

        Assert.That((await grain.GetAdmissionAsync(OriginA)).HighWaterMark, Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task An_installed_floor_drops_below_the_watermark_except_held_writes_and_is_durable()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        IReplicationHighWaterMarkGrain grain = CreateGrain(state);

        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held((OriginA, [Hlc(40)])));
        var admission = await grain.GetAdmissionAsync(OriginA);
        var other = await grain.GetAdmissionAsync(OriginB);
        var restarted = await ((IReplicationHighWaterMarkGrain)CreateGrain(state)).GetAdmissionAsync(OriginA);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Drops(Hlc(50)), Is.True, "below the source's watermark and not held: reflected by the export");
            Assert.That(admission.Drops(Hlc(40)), Is.False, "held at the source: the export lacks it, so it must apply");
            Assert.That(admission.Drops(Hlc(100)), Is.False, "the watermark is strict");
            Assert.That(admission.Drops(Hlc(150)), Is.False);
            Assert.That(other.Drops(Hlc(1)), Is.False, "an origin the export carries no watermark for has no floor");
            Assert.That(restarted.Drops(Hlc(50)), Is.True, "the floor survives a reactivation");
        });
    }

    [Test]
    public async Task Every_install_bumps_a_durable_floor_epoch_the_admission_carries()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        IReplicationHighWaterMarkGrain grain = CreateGrain(state);

        var fresh = (await grain.GetAdmissionAsync(OriginA)).FloorEpoch;
        var first = await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());
        await grain.ClearBootstrapFloorAsync();
        var second = await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());
        var carried = (await grain.GetAdmissionAsync(OriginB)).FloorEpoch;
        var restarted = (await ((IReplicationHighWaterMarkGrain)CreateGrain(state)).GetAdmissionAsync(OriginA)).FloorEpoch;

        Assert.Multiple(() =>
        {
            Assert.That(fresh, Is.Zero);
            Assert.That(first, Is.EqualTo(1));
            Assert.That(second, Is.EqualTo(2), "a clear never lowers the epoch");
            Assert.That(carried, Is.EqualTo(2), "every origin's admission carries it, floored or not");
            Assert.That(restarted, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_floor_is_provisional_until_finalized_and_finalizing_is_durable()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        IReplicationHighWaterMarkGrain grain = CreateGrain(state);

        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());
        var installed = await grain.GetAdmissionAsync(OriginA);
        await grain.FinalizeBootstrapFloorAsync();
        var finalized = await grain.GetAdmissionAsync(OriginA);
        var restarted = await ((IReplicationHighWaterMarkGrain)CreateGrain(state)).GetAdmissionAsync(OriginA);
        await grain.ClearBootstrapFloorAsync();
        await grain.FinalizeBootstrapFloorAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(installed.FloorProvisional, Is.True, "an import that has not closed defers below its floor");
            Assert.That(finalized.FloorProvisional, Is.False);
            Assert.That(finalized.Drops(Hlc(50)), Is.True);
            Assert.That(restarted.FloorProvisional, Is.False);
            Assert.That((await grain.GetAdmissionAsync(OriginA)).Drops(Hlc(50)), Is.False, "finalizing no floor is a no-op");
        });
    }

    [Test]
    public async Task ResetAppliedIdentitiesAsync_clears_the_floor_durably()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        IReplicationHighWaterMarkGrain grain = CreateGrain(state);
        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());

        await grain.ResetAppliedIdentitiesAsync();

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetAdmissionAsync(OriginA)).Drops(Hlc(50)), Is.False,
                "the floor vouches for contents that were just replaced");
            Assert.That(state.State.BootstrapFloor, Is.Null);
        });
    }

    [Test]
    public async Task A_failed_write_while_clearing_the_floor_propagates_and_keeps_it()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        IReplicationHighWaterMarkGrain grain = CreateGrain(state);
        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(async () => await grain.ResetAppliedIdentitiesAsync(), Throws.InvalidOperationException,
            "the replacement must not proceed with the floor still durable");
        state.ThrowOnWrite = null;
        Assert.That((await grain.GetAdmissionAsync(OriginA)).Drops(Hlc(50)), Is.True);
    }

    [Test]
    public async Task A_later_install_replaces_the_floor_and_ClearBootstrapFloorAsync_removes_it()
    {
        IReplicationHighWaterMarkGrain grain = CreateGrain();
        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(100))), Held());

        await grain.SetBootstrapFloorAsync(Watermarks((OriginB, Hlc(10))), Held());
        var replaced = (await grain.GetAdmissionAsync(OriginA), await grain.GetAdmissionAsync(OriginB));
        await grain.ClearBootstrapFloorAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(replaced.Item1.Drops(Hlc(50)), Is.False);
            Assert.That(replaced.Item2.Drops(Hlc(5)), Is.True);
            Assert.That((await grain.GetAdmissionAsync(OriginB)).Drops(Hlc(5)), Is.False);
        });
    }

    [Test]
    public async Task An_export_out_of_bounds_installs_no_floor()
    {
        IReplicationHighWaterMarkGrain grain = CreateGrain();
        var tooMany = Enumerable.Range(0, ReplicationBootstrapFloor.MaxHeldPerOrigin + 1).Select(i => Hlc(i + 1)).ToArray();

        await grain.SetBootstrapFloorAsync(Watermarks((OriginA, Hlc(int.MaxValue))), Held((OriginA, tooMany)));

        Assert.That((await grain.GetAdmissionAsync(OriginA)).Drops(Hlc(1)), Is.False,
            "an export over the held bound drops nothing, rather than drop a held write");
    }
}
