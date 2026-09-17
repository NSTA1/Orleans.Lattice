using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression cover for issue 3117: a division must never mint a leaf that
/// declares an empty key range.
/// <para>
/// A split hands <c>[Low, pivot)</c> to the donor and <c>[pivot, High)</c> to
/// the sibling, so a pivot outside <c>(Low, High)</c> leaves one of the two
/// halves declaring a range that contains no key at all. Such a leaf is not
/// inert: it still holds rows and still answers reads, but no routing descent
/// can ever select it, because every descent tests the key against exactly that
/// range. Writes go to the real custodian while reads can be served the
/// marooned copy, which presents as a key frozen at an old value while its
/// siblings advance - a torn read that never heals.
/// </para>
/// <para>
/// The pivot is drawn from the row set rather than from the range, so it is only
/// as sound as the row set is, and a leaf may legitimately hold rows outside its
/// own span: span admission forwards on a best-effort basis and deliberately
/// commits locally when no forward target resolves (see
/// <c>BPlusLeafGrain.SpanAdmission.cs</c>), and a cross-shard migration grafts
/// rows ahead of the range fixup. Bisecting such a leaf can therefore select an
/// out-of-span key.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int PivotAdmissibilityMaxLeafKeys = 4;

    /// <summary>
    /// Captures the payload the donor seeds its new sibling with, which is where
    /// the defective range became durable.
    /// </summary>
    private static IBPlusLeafGrain CreateCapturingSiblingStub(
        out Func<SiblingInitialization?> captured)
    {
        SiblingInitialization? seen = null;

        // Also substitute IGrainBase: SplitAsync mints the sibling id through
        // GrainExtensions.GetGrainId, which resolves a plain proxy to nothing and
        // throws. Routing it through a stubbed IGrainContext is what lets the
        // forward split path run at all under a substituted factory.
        var stub = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)stub).GrainContext.Returns(siblingContext);

        stub.InitializeSiblingAsync(Arg.Do<SiblingInitialization>(init => seen = init))
            .Returns(Task.CompletedTask);
        captured = () => seen;
        return stub;
    }

    /// <summary>
    /// The bisector picks the ordinal-middle row. When that row sits at or past
    /// the leaf's own <c>HighKeyExclusive</c>, using it verbatim would seed the
    /// sibling with <c>[k60, k50)</c> - a range whose low bound exceeds its high
    /// bound, so it declares nothing. The division must fall back to a pivot
    /// drawn from the rows the leaf actually owns.
    /// <para>
    /// Perturbation: remove the admissibility check in <c>SplitAsync</c> and the
    /// captured low bound becomes <c>k60</c>, failing the range assertion below.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_repairs_a_pivot_at_or_past_the_high_bound_rather_than_seeding_an_empty_sibling_range()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateCapturingSiblingStub(out var captured);
        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: PivotAdmissibilityMaxLeafKeys);

        state.State.LowKeyInclusive = "k30";
        state.State.HighKeyExclusive = "k50";

        // k30 and k35 are owned; k60, k70 and k80 are orphans parked here by the
        // fail-open span-admission path. Sorted, the middle row is k60 - out of
        // span, and the pivot the unrepaired bisector would have used.
        foreach (var key in new[] { "k30", "k35", "k60", "k70", "k80" })
        {
            await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var init = captured();
        Assert.That(init, Is.Not.Null, "the leaf was over capacity and should have divided");

        Assert.Multiple(() =>
        {
            Assert.That(
                // Compared against the literal pre-split bound: CompleteSplitAsync
                // narrows the donor's own HighKeyExclusive to the pivot, so reading
                // it back from state here would compare the pivot with itself.
                string.CompareOrdinal(init!.Value.LowKeyInclusive, "k50"),
                Is.LessThan(0),
                "the sibling's low bound must stay below the donor's pre-split high bound, "
                + "or the sibling declares an empty range and can never be routed a write");
            Assert.That(
                string.CompareOrdinal(init!.Value.LowKeyInclusive, "k30"),
                Is.GreaterThan(0),
                "the sibling's low bound must stay above the donor's low bound, "
                + "or the donor is left declaring an empty range instead");
            Assert.That(init!.Value.LowKeyInclusive, Is.EqualTo("k35"), "the only admissible pivot");
        });
    }

    /// <summary>
    /// The mirror case, and the one <c>SplitBoundary.Owns</c> would wave
    /// through: a pivot exactly equal to <c>LowKeyInclusive</c> is in span, yet
    /// it leaves the <i>donor</i> declaring <c>[k30, k30)</c>. Admissibility is
    /// therefore strictly stronger than ownership.
    /// <para>
    /// Perturbation: relax <c>IsAdmissibleSplitPivot</c>'s low-bound test from
    /// <c>&gt; 0</c> to <c>&gt;= 0</c> and the donor is left with an empty range.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_repairs_a_pivot_equal_to_the_low_bound_rather_than_leaving_the_donor_empty()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateCapturingSiblingStub(out var captured);
        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: PivotAdmissibilityMaxLeafKeys);

        state.State.LowKeyInclusive = "k30";
        state.State.HighKeyExclusive = "k99";

        // Sorted, the middle row is k30 - in span, but equal to the low bound.
        foreach (var key in new[] { "k10", "k20", "k30", "k40", "k50" })
        {
            await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var init = captured();
        Assert.That(init, Is.Not.Null, "the leaf was over capacity and should have divided");
        Assert.That(
            string.CompareOrdinal(init!.Value.LowKeyInclusive, "k30"),
            Is.GreaterThan(0),
            "a pivot equal to the donor's low bound leaves the donor declaring an empty range");
    }

    /// <summary>
    /// When every row is an orphan there is nothing the leaf owns to divide, and
    /// any pivot at all would strand one half. Declining is the correct outcome
    /// and is self-correcting: the orphans drain to their real custodians, after
    /// which the leaf is either back under threshold or divisible.
    /// <para>
    /// Perturbation: remove the decline branch and the division proceeds on an
    /// out-of-span pivot, seeding a sibling no descent can reach.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_declines_when_no_row_falls_inside_the_declared_range()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateCapturingSiblingStub(out var captured);
        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: PivotAdmissibilityMaxLeafKeys);

        state.State.LowKeyInclusive = "k30";
        state.State.HighKeyExclusive = "k50";

        SplitResult? last = null;
        foreach (var key in new[] { "k60", "k70", "k80", "k90", "k95" })
        {
            last = await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        Assert.Multiple(() =>
        {
            Assert.That(last, Is.Null, "a leaf holding only orphan rows has nothing to divide");
            Assert.That(
                captured(),
                Is.Null,
                "no sibling may be seeded when no admissible pivot exists");
            Assert.That(
                state.State.SplitState,
                Is.EqualTo(Orleans.Lattice.Primitives.SplitState.Unsplit),
                "a declined division must not leave split intent behind");
        });
    }

    /// <summary>
    /// The decline must be <em>observable</em>, not merely correct. A leaf that
    /// silently refuses to divide is indistinguishable from one nothing is
    /// trying to divide, which is the exact ambiguity
    /// <see cref="LatticeMetrics.LeafSplitAttempts"/> exists to remove - so the
    /// decline records its own outcome arm rather than returning quietly.
    /// <para>
    /// The arm is also pre-minted at zero on the capture seam, so an operator
    /// reading a flat zero learns "no division was ever declined" rather than
    /// "this build predates the guard". Both halves are needed: this test pins
    /// the emission, and
    /// <c>LeafSplitAttemptAccountingTests.The_capture_seam_mints_every_outcome_at_zero_so_never_sought_is_readable</c>
    /// pins the prime.
    /// </para>
    /// <para>
    /// Perturbation: drop the <c>RecordSplitAttempt</c> call on the decline
    /// branch and the measurement list holds no non-zero
    /// <c>no_admissible_pivot</c>, failing below while every other arm of this
    /// file still passes.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_records_the_no_admissible_pivot_outcome_when_it_declines()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateCapturingSiblingStub(out _);
        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: PivotAdmissibilityMaxLeafKeys);

        state.State.LowKeyInclusive = "k30";
        state.State.HighKeyExclusive = "k50";

        var declines = 0;
        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitAttempts,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome
                        && string.Equals(
                            tag.Value?.ToString(),
                            LatticeMetrics.LeafSplitNoAdmissiblePivot.Value?.ToString(),
                            StringComparison.Ordinal)
                        && value != 0)
                    {
                        Interlocked.Increment(ref declines);
                    }
                }
            })))
        {
            foreach (var key in new[] { "k60", "k70", "k80", "k90", "k95" })
            {
                await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));
            }
        }

        Assert.That(
            declines,
            Is.GreaterThan(0),
            "a declined division must be readable, or it is indistinguishable from a division "
            + "nothing ever sought - which is the ambiguity the attempt counter exists to remove");
    }

    /// <summary>
    /// The ordinary shape is unaffected: an in-span median is used verbatim, so
    /// the repair path costs nothing on a healthy leaf.
    /// </summary>
    [Test]
    public async Task Split_uses_an_in_span_median_unchanged()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateCapturingSiblingStub(out var captured);
        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: PivotAdmissibilityMaxLeafKeys);

        state.State.LowKeyInclusive = "k10";
        state.State.HighKeyExclusive = "k99";

        foreach (var key in new[] { "k10", "k20", "k30", "k40", "k50" })
        {
            await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var init = captured();
        Assert.That(init, Is.Not.Null);
        Assert.That(init!.Value.LowKeyInclusive, Is.EqualTo("k30"), "the unmodified ordinal median");
    }
}
