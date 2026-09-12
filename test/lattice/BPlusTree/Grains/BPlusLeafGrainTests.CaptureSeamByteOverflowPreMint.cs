using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Zero-priming for <c>leaf_byte_overflow_total</c> (issue #2756).
/// <para>
/// A <see cref="Counter{T}"/> exports no series at all until its first
/// <c>Add</c>, so before this change an absent <c>leaf_byte_overflow_total</c>
/// spanned three states a reader cannot tell apart: the hoist of the pre-split
/// to the capture seam (issue #2733) did not land, or it landed and no leaf is
/// oversized, or no leaf has activated yet.
/// </para>
/// <para>
/// That ambiguity is not hypothetical. The absence of this exact series was
/// read on a live deployment as evidence the capture-seam hoist had failed,
/// and was adopted as a go/no-go criterion for a redeploy - when the same
/// absence was equally consistent with the hoist having worked and left
/// nothing to divide. A counter that is only ever <c>Add(1)</c> cannot
/// distinguish "not happening" from "not deployed" from "nothing to do", and
/// is therefore unfit as a stop criterion however well it serves as evidence
/// when it fires.
/// </para>
/// <para>
/// Priming at the pre-split rather than at activation is deliberate: that
/// method is reached only through <c>CaptureSnapshotCoreAsync</c>, so a minted
/// zero is a positive statement that the capture seam ran and evaluated the
/// byte bound - which is the property actually in question - and an absent
/// series means the seam was never reached rather than that no leaf overflowed.
/// </para>
/// </summary>
public sealed partial class BPlusLeafGrainCaptureSeamByteOverflowTests
{
    /// <summary>
    /// Collects every measurement <paramref name="instrument"/> publishes while
    /// <paramref name="body"/> runs, with each measurement's tags flattened.
    /// <para>
    /// Uses <see cref="MeterListening.StartForInstrument"/>, which takes the
    /// instrument as a parameter and so forces the owning type initialiser to
    /// complete BEFORE the listener exists. The meter-field ordering hazard
    /// that silently disables an instrument during publication is therefore not
    /// expressible here.
    /// </para>
    /// </summary>
    private static async Task<List<(long Value, Dictionary<string, object?> Tags)>> RecordByteOverflowAsync(
        Counter<long> instrument,
        Func<Task> body)
    {
        var measurements = new List<(long, Dictionary<string, object?>)>();
        var gate = new Lock();

        using var listener = MeterListening.StartForInstrument(
            instrument,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var flattened = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var tag in tags)
                    flattened[tag.Key] = tag.Value;

                lock (gate)
                    measurements.Add((value, flattened));
            }));

        await body();

        lock (gate)
            return [.. measurements];
    }

    private static object? OutcomeOf(Dictionary<string, object?> tags)
        => tags.TryGetValue(LatticeMetrics.TagOutcome, out var outcome) ? outcome : null;

    /// <summary>
    /// The core clause: a leaf that reaches the capture seam and needs NO
    /// division still mints both outcomes at zero.
    /// <para>
    /// This is the arm that converts absence into information. Without the
    /// prime this leaf emits nothing whatsoever, which is indistinguishable
    /// from a build in which the pre-split was never wired.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_under_bound_leaf_reaching_the_capture_seam_mints_both_outcomes_at_zero()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 1024 * 1024, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        var measurements = await RecordByteOverflowAsync(
            LatticeMetrics.LeafByteOverflows,
            async () => await h.Grain.CaptureSnapshotAsync());

        var mintedOutcomes = measurements
            .Where(m => m.Value == 0)
            .Select(m => OutcomeOf(m.Tags))
            .Distinct()
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements, Is.Not.Empty,
                "the capture seam published no byte-overflow measurement at all for a leaf that is "
                + "comfortably under the bound. An absent series is then attributable to the build "
                + "rather than to the leaf, which is the ambiguity issue #2756 exists to remove.");
            Assert.That(
                mintedOutcomes,
                Is.EquivalentTo(new[]
                {
                    LatticeMetrics.LeafByteOverflowSplit.Value,
                    LatticeMetrics.LeafByteOverflowIrreducible.Value,
                }),
                "both outcomes must be minted, not just the one this leaf happened not to hit. A "
                + "reader comparing split against irreducible needs both lines to exist.");
            Assert.That(
                measurements.Select(m => m.Value), Is.All.Zero,
                "an under-bound leaf must not record a real overflow - the prime may mint the "
                + "series but must never move it.");
        });
    }

    /// <summary>
    /// The tag-shape clause, and the one whose failure would leave the tree
    /// WORSE off than no prime at all.
    /// <para>
    /// A prime emitted on a different tag shape from the real emission mints a
    /// second series that never converges with the one actually counting: a
    /// permanently-zero line sitting beside a live counter, which reads as a
    /// measured zero and is actively misleading. The production code guarantees
    /// this structurally by routing both the prime and the real emission
    /// through the same recorder; this arm pins that guarantee so a future
    /// refactor cannot quietly split them.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_primed_series_carries_the_same_tag_set_as_a_real_emission()
    {
        // 16 KiB against a 4 KiB bound, so a real `split` emission is produced
        // alongside the prime and the two tag sets can be compared directly.
        var h = CreateOversizedLeaf(maxLeafBytes: 4096, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        var measurements = await RecordByteOverflowAsync(
            LatticeMetrics.LeafByteOverflows,
            async () => await h.Grain.CaptureSnapshotAsync());

        var primed = measurements.Where(m =>
            m.Value == 0
            && Equals(OutcomeOf(m.Tags), LatticeMetrics.LeafByteOverflowSplit.Value))
            .Select(m => m.Tags)
            .FirstOrDefault();

        var real = measurements.Where(m =>
            m.Value > 0
            && Equals(OutcomeOf(m.Tags), LatticeMetrics.LeafByteOverflowSplit.Value))
            .Select(m => m.Tags)
            .FirstOrDefault();

        Assert.That(primed, Is.Not.Null, "no zero-primed `split` series was minted.");
        Assert.That(
            real, Is.Not.Null,
            "no real `split` emission was produced, so this arm cannot compare tag shapes. The "
            + "leaf is 4x over the bound and must have divided.");

        Assert.That(
            primed, Is.EquivalentTo(real!),
            "the primed series and the real emission carry DIFFERENT tag sets, so they are two "
            + "distinct series. The prime would then show a permanently-flat zero next to a "
            + "counter that is separately counting - worse than absence, because a reader takes "
            + "it for a measured zero.");
    }

    /// <summary>
    /// The prime must precede the early return, not follow it.
    /// <para>
    /// Distinct from the under-bound arm because it fails for a different
    /// reason: with the bound DISABLED the method returns before evaluating
    /// anything, so a prime placed after that guard would emit nothing and the
    /// operator loses the ability to tell a disabled bound from an unwired
    /// build - exactly when they most need it, since a disabled bound is a
    /// configuration state somebody chose and should be able to confirm.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_leaf_whose_byte_bound_is_disabled_still_mints_the_series()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 0, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        var measurements = await RecordByteOverflowAsync(
            LatticeMetrics.LeafByteOverflows,
            async () => await h.Grain.CaptureSnapshotAsync());

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements, Is.Not.Empty,
                "with MaxLeafBytes = 0 the pre-split returns immediately, and nothing was minted. "
                + "The prime has to sit ABOVE that early return or a disabled bound is "
                + "indistinguishable from a build that never wired the instrument.");
            Assert.That(
                measurements.Select(m => m.Value), Is.All.Zero,
                "a disabled bound must never record a real overflow.");
        });
    }
}
