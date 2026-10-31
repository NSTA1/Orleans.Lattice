using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The removal fence of <see cref="WalMaterialiserPinGrain"/> (issue #5358):
/// <see cref="WalMaterialiserPinGrain.RemoveIfUnmergedSinceAsync"/> refuses a
/// WAL GC retirement decided on a read the consumer has been merged past.
/// </summary>
public sealed partial class WalMaterialiserPinGrainTests
{
    [Test]
    public async Task RemoveIfUnmergedSinceAsync_removes_a_pin_not_merged_since_the_token()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var token = await grain.GetRetirementTokenAsync();

        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, token);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.False);
        });
    }

    [Test]
    public async Task RemoveIfUnmergedSinceAsync_refuses_after_a_seed_that_changes_nothing()
    {
        // The #5358 interleaving at the pin store: a purge-cleared leaf's pin
        // survives; the retirement takes its token and reads the leaf as a husk;
        // the leaf is re-created under the same grain id and its birth seed of a
        // Zero block pin lands in the surviving entry, where the monotone merge
        // changes nothing a read could see. The stale removal must be refused.
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var token = await grain.GetRetirementTokenAsync();

        await grain.SeedManyAsync([new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1)]);
        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, token);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)),
                "anti-vacuity: the seed must have changed nothing the pins show.");
            Assert.That(removed, Is.False, "the fence must refuse a removal decided before the seed.");
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True, "the re-created leaf keeps its pin.");
        });
    }

    [Test]
    public async Task RemoveIfUnmergedSinceAsync_refuses_after_a_report_or_a_hold()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var beforeReport = await grain.GetRetirementTokenAsync();
        await grain.ReportAsync(Consumer, Hlc(50));
        var beforeHold = await grain.GetRetirementTokenAsync();
        await grain.RaiseOverrideHoldsAsync([Consumer]);

        var afterReport = await grain.RemoveIfUnmergedSinceAsync(Consumer, beforeReport);
        var afterHold = await grain.RemoveIfUnmergedSinceAsync(Consumer, beforeHold);

        Assert.Multiple(() =>
        {
            Assert.That(afterReport, Is.False);
            Assert.That(afterHold, Is.False);
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True);
        });
    }

    [Test]
    public async Task RemoveIfUnmergedSinceAsync_is_not_fenced_by_another_consumers_merge()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var token = await grain.GetRetirementTokenAsync();
        await grain.ReportAsync(Consumer + "_other", Hlc(100));

        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, token);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.False);
            Assert.That(state.State.Pins.ContainsKey(Consumer + "_other"), Is.True);
        });
    }

    [Test]
    public async Task RemoveIfUnmergedSinceAsync_refuses_a_token_from_another_activation()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var (other, _) = CreateGrain();
        var foreign = await other.GetRetirementTokenAsync();

        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, foreign);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.False);
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True);
        });
    }

    [Test]
    public async Task RemoveIfUnmergedSinceAsync_of_an_absent_pin_succeeds_without_a_write()
    {
        var (grain, state) = CreateGrain();
        var token = await grain.GetRetirementTokenAsync();

        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, token);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_merge_after_a_removal_refuses_every_token_taken_before_it()
    {
        // The stamp is forgotten on removal; a later merge must still refuse a
        // token that predates it.
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        var early = await grain.GetRetirementTokenAsync();
        Assert.That(await grain.RemoveIfUnmergedSinceAsync(Consumer, early), Is.True);
        await grain.SeedManyAsync([new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1)]);

        var removed = await grain.RemoveIfUnmergedSinceAsync(Consumer, early);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.False);
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True);
        });
    }
}
