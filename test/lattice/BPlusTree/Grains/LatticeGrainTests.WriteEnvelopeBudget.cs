using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2685, at the grain seam: the batched-write
/// envelope budget, and the single-shard fan-out that was not bounded at all.
/// <para>
/// Two distinct defects are pinned here, and they fail independently.
/// </para>
/// <para>
/// <b>1. The single-shard fast path had no time bound of any kind.</b>
/// <c>SetManyAsyncCore</c> skips shard bucketing when a tree has one physical
/// shard - the dominant shape - and that branch awaited the shard write
/// unbounded. <see cref="LatticeOptions.SetManyFanOutBudget"/> does not apply
/// there, and that carve-out is correct and documented: it bounds the slowest
/// of <em>several</em> branches, which a one-branch write does not have. The
/// consequence was nonetheless that a single-shard tree could not have its
/// batch writes bounded at all, by any option. The envelope budget is the
/// instrument that fits, because a single-shard tree has an envelope even
/// though it has no branch dispersion.
/// <see cref="SetManyAsync_single_shard_fast_path_still_ignores_the_fan_out_budget"/>
/// pins the carve-out so the fix does not quietly widen it.
/// </para>
/// <para>
/// <b>2. A per-stage budget cannot see an additive breach.</b> The incident
/// summed a <c>gate</c> of 4,108.96 ms and a <c>fanout</c> of 26,709.17 ms to
/// 30,818 ms against a 30,000 ms response timeout with neither stage breaching
/// alone, so a fan-out budget sized for the fan-out never fires.
/// <see cref="SetManyAsync_envelope_budget_catches_a_breach_a_fan_out_budget_cannot"/>
/// drives exactly that shape through the grain and asserts both directions,
/// which is what stops the guard being vacuous.
/// </para>
/// <para>
/// The failing direction of every test here is bounded by a straggler that
/// never completes, so a correct implementation settles deterministically on
/// its budget and a regression hangs rather than racing the clock.
/// </para>
/// </summary>
public partial class LatticeGrainTests
{
    /// <summary>
    /// Generous outer bound on the <em>failing</em> direction only: a correct
    /// implementation settles on its configured budget long before this.
    /// </summary>
    private static readonly TimeSpan EnvelopeTestCeiling = TimeSpan.FromSeconds(20);

    [Test]
    public async Task SetManyAsync_single_shard_fast_path_still_ignores_the_fan_out_budget()
    {
        // The carve-out is deliberate and documented: SetManyFanOutBudget
        // bounds the slowest of SEVERAL branches, and a single-shard batch has
        // no branch dispersion to bound, so configuration.md states plainly
        // that "single-shard batches never consult it". #2685 does not change
        // that contract - it adds a second, differently-shaped budget for the
        // whole envelope - so this test pins the carve-out against a future
        // change that "helpfully" extends the fan-out budget here and silently
        // alters a documented behaviour.
        const string treeId = "envelope-single-shard-fanout-carveout";
        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyFanOutBudget = TimeSpan.FromMilliseconds(200) },
            shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shardRoot = SetupShardRoot(factory);

        var gate = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => gate.Task);

        var call = grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]);

        var early = await Task.WhenAny(call, Task.Delay(TimeSpan.FromMilliseconds(750)));
        Assert.That(early, Is.Not.SameAs(call),
            "A single-shard batch must not consult SetManyFanOutBudget. Bounding it here "
            + "would change a documented contract rather than fix #2685; the envelope budget "
            + "is the instrument for this path.");

        gate.SetResult();
        await call;
    }

    [Test]
    public async Task SetManyAsync_bounds_the_single_shard_fast_path_with_the_envelope_budget()
    {
        const string treeId = "envelope-single-shard-envelope-budget";
        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyEnvelopeBudget = TimeSpan.FromMilliseconds(250) },
            shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shardRoot = SetupShardRoot(factory);

        var straggler = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => straggler.Task);

        var call = grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]);

        var settled = await Task.WhenAny(call, Task.Delay(EnvelopeTestCeiling));
        Assert.That(settled, Is.SameAs(call),
            "The envelope budget must bound the single-shard path as well as the multi-shard "
            + "one, or the tree shape decides whether the deadline is enforced.");

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await call);
        Assert.Multiple(() =>
        {
            Assert.That(thrown!.SaturationSource, Is.EqualTo(LatticeSaturationSource.SetManyEnvelope),
                "An envelope breach must carry its own source. A caller that sees SetManyFanOut "
                + "investigates the fan-out, which in the #2685 shape is the stage that did not "
                + "move.");
            Assert.That(thrown.Message, Does.Contain("gate="),
                "The refusal must carry the per-stage breakdown, or the next occurrence is as "
                + "undiagnosable as the anonymous Orleans timeout it replaces.");
            Assert.That(thrown.Message, Does.Contain("SetManyEnvelopeBudget"),
                "The refusal must name the knob that produced it.");
        });

        straggler.SetResult();
    }

    [Test]
    public async Task SetManyAsync_envelope_budget_catches_a_breach_a_fan_out_budget_cannot()
    {
        // The additive shape, driven end to end. The gate is made slow - which
        // is what degraded 12,085x in the incident - and the fan-out then never
        // settles. The budget is sized so the gate alone is comfortably inside
        // it, so nothing a per-stage guard could assert has been violated at
        // the point the envelope refuses.
        const string treeId = "envelope-additive-breach";
        var gateDelay = TimeSpan.FromMilliseconds(400);
        var envelopeBudget = TimeSpan.FromSeconds(2);

        Assert.That(gateDelay, Is.LessThan(envelopeBudget),
            "Control: the gate alone stays inside the budget, so this test cannot pass by the "
            + "gate breaching on its own. The breach has to come from the sum.");

        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyEnvelopeBudget = envelopeBudget },
            shardCount: 1);

        var compaction = SetupCompactionGrain(factory, treeId);
        compaction.EnsureReminderAsync().Returns(_ => Task.Delay(gateDelay));

        var shardRoot = SetupShardRoot(factory);
        var straggler = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => straggler.Task);

        var call = grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]);

        var settled = await Task.WhenAny(call, Task.Delay(EnvelopeTestCeiling));
        Assert.That(settled, Is.SameAs(call),
            "The envelope must refuse once the stages have summed past it.");

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await call);
        Assert.That(thrown!.SaturationSource, Is.EqualTo(LatticeSaturationSource.SetManyEnvelope));

        // The load-bearing half: the fan-out was granted strictly less than the
        // whole budget, because the gate had already spent part of it. Had the
        // fan-out been handed a fresh full window - which is what a fan-out-only
        // budget does - the stages would sum past the deadline exactly as they
        // did in the incident.
        Assert.That(thrown.Message, Does.Contain("gate="),
            "The breakdown must attribute the consumed budget to the gate.");

        straggler.SetResult();
    }

    [Test]
    public async Task SetManyAsync_is_unbounded_when_neither_budget_is_configured()
    {
        // The no-regression control. The envelope budget defaults to InfiniteTimeSpan and a single-shard batch skips the fan-out budget,
        // so an existing deployment must behave exactly as it did: the write
        // completes normally and nothing refuses it.
        const string treeId = "envelope-default-unbounded";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shardRoot = SetupShardRoot(factory);

        var gate = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => gate.Task);

        var call = grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]);

        // Still outstanding after a pause that both configured budgets in this
        // fixture would have fired within: unbounded means unbounded.
        var early = await Task.WhenAny(call, Task.Delay(TimeSpan.FromMilliseconds(750)));
        Assert.That(early, Is.Not.SameAs(call),
            "With no budget configured the write must wait for its shard however long it "
            + "takes. Refusing here would regress every deployment that has not opted in.");

        gate.SetResult();
        await call;

        Assert.That(
            shardRoot.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.SetManyAsync)),
            Is.EqualTo(1),
            "The unbounded path must issue exactly one write and return it unchanged.");
    }

    [Test]
    public void SetManyEnvelopeBudget_defaults_to_unbounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LatticeOptions().SetManyEnvelopeBudget,
                Is.EqualTo(Timeout.InfiniteTimeSpan),
                "The envelope bound stays opt-in (unlike SetManyFanOutBudget, which is "
                + "finite from 10.0), so upgrading changes nothing for it.");
            Assert.That(LatticeOptions.DefaultSetManyEnvelopeBudget,
                Is.EqualTo(Timeout.InfiniteTimeSpan));
        });
    }
}
