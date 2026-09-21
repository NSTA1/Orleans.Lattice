using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="SplitPivotAdmission"/>, the pure core that decides
/// whether a leaf may be divided at a given key. The grain-level fixture proves the
/// split path consults it and the Coyote model proves the property it guarantees;
/// these pin the core's own contract, and in particular the two asymmetries that are
/// easy to get wrong.
/// <para>
/// The first is that admissibility is strictly stronger than ownership on the low
/// side. <see cref="SplitBoundary.Owns"/> admits <c>key == low</c> because the range
/// is half-open, but a pivot equal to the low bound narrows the donor to
/// <c>[Low, Low)</c> - an empty range - so the split rule must use <c>&gt;</c> where
/// the routing rule uses <c>&gt;=</c>. Reusing <c>Owns</c> here would look correct
/// and silently maroon the donor.
/// </para>
/// <para>
/// The second is that a null bound is unbounded, not absent: it constrains nothing
/// and so can never make a pivot inadmissible.
/// </para>
/// </summary>
[TestFixture]
public sealed class SplitPivotAdmissionTests
{
    [Test]
    public void IsAdmissible_accepts_a_key_strictly_inside_the_declared_range()
    {
        Assert.That(SplitPivotAdmission.IsAdmissible("k20", "k10", "k30"), Is.True);
    }

    [Test]
    public void IsAdmissible_rejects_a_key_equal_to_the_low_bound()
    {
        // Owns("k10", "k10", "k30") is true - the low bound is inclusive for routing.
        // Admissibility must still reject it, or the donor is left declaring [k10,k10).
        Assert.Multiple(() =>
        {
            Assert.That(SplitBoundary.Owns("k10", "k10", "k30"), Is.True);
            Assert.That(SplitPivotAdmission.IsAdmissible("k10", "k10", "k30"), Is.False);
        });
    }

    [Test]
    public void IsAdmissible_rejects_a_key_at_or_past_the_high_bound()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SplitPivotAdmission.IsAdmissible("k30", "k10", "k30"), Is.False);
            Assert.That(SplitPivotAdmission.IsAdmissible("k40", "k10", "k30"), Is.False);
        });
    }

    [Test]
    public void IsAdmissible_rejects_a_key_below_the_low_bound()
    {
        Assert.That(SplitPivotAdmission.IsAdmissible("k05", "k10", "k30"), Is.False);
    }

    [Test]
    public void IsAdmissible_rejects_a_null_pivot()
    {
        Assert.That(SplitPivotAdmission.IsAdmissible(null, "k10", "k30"), Is.False);
    }

    [Test]
    public void IsAdmissible_treats_a_null_bound_as_unbounded_rather_than_absent()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SplitPivotAdmission.IsAdmissible("k05", null, "k30"), Is.True);
            Assert.That(SplitPivotAdmission.IsAdmissible("k99", "k10", null), Is.True);
            Assert.That(SplitPivotAdmission.IsAdmissible("k00", null, null), Is.True);
        });
    }

    [Test]
    public void SelectMedianAdmissible_returns_the_median_of_the_in_span_keys_only()
    {
        // k05 and k40 are out of span, so the admissible subset is k15/k20/k25 and
        // its median is k20 - not the median of the whole row set, which is k20 only
        // by coincidence in a symmetric set. The asymmetric set below removes that.
        var keys = new[] { "k05", "k15", "k20", "k25", "k40", "k41" };

        Assert.That(
            SplitPivotAdmission.SelectMedianAdmissible(keys, "k10", "k30"),
            Is.EqualTo("k20"));
    }

    [Test]
    public void SelectMedianAdmissible_selects_the_upper_median_of_an_even_count()
    {
        var keys = new[] { "k15", "k20" };

        Assert.That(
            SplitPivotAdmission.SelectMedianAdmissible(keys, "k10", "k30"),
            Is.EqualTo("k20"));
    }

    [Test]
    public void SelectMedianAdmissible_returns_null_when_no_key_is_admissible()
    {
        // Every key is out of span, so the leaf owns nothing it is entitled to
        // divide. Null is the decline signal; returning a least-bad key here would
        // reintroduce the very defect the guard exists to prevent.
        var keys = new[] { "k30", "k40" };

        Assert.That(SplitPivotAdmission.SelectMedianAdmissible(keys, "k10", "k30"), Is.Null);
    }

    [Test]
    public void SelectMedianAdmissible_returns_null_for_an_empty_row_set()
    {
        Assert.That(
            SplitPivotAdmission.SelectMedianAdmissible([], "k10", "k30"),
            Is.Null);
    }

    [Test]
    public void SelectMedianAdmissible_rejects_a_null_key_sequence()
    {
        Assert.That(
            () => SplitPivotAdmission.SelectMedianAdmissible(null!, "k10", "k30"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void SelectMedianAdmissible_only_returns_an_admissible_key()
    {
        // The guarantee the split path relies on: whatever comes back may be applied
        // without re-validation.
        var keys = new[] { "k05", "k10", "k15", "k30", "k99" };

        var pivot = SplitPivotAdmission.SelectMedianAdmissible(keys, "k10", "k30");

        Assert.That(SplitPivotAdmission.IsAdmissible(pivot, "k10", "k30"), Is.True);
    }
}
