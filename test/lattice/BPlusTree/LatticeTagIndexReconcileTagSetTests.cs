using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the behaviour of <see cref="LatticeTagIndexContext.ReconcileTagSet"/>
/// across the width at which it switches strategy.
/// </summary>
/// <remarks>
/// <para>
/// The method deduplicates the desired tag list with an ordinal linear scan at
/// or below <see cref="LatticeTagIndexContext.LinearTagScanThreshold"/> and with
/// a <see cref="HashSet{T}"/> above it, so that the common narrow write does not
/// pay for a set's bucket and entry arrays. Two branches answering the same
/// question is exactly the shape that drifts silently, so every assertion below
/// is written to hold identically on both sides of the threshold, and the widths
/// exercised straddle it.
/// </para>
/// <para>
/// The ordering assertions are load-bearing rather than incidental. The hashing
/// branch enumerates its <see cref="HashSet{T}"/>, which preserves insertion
/// order only while no element has been removed; the linear branch preserves
/// first-seen order by construction. Pinning first-seen order on both keeps the
/// two observationally identical, so the partitions a caller receives do not
/// depend on which branch ran.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeTagIndexReconcileTagSetTests
{
    /// <summary>
    /// Widths straddling <see cref="LatticeTagIndexContext.LinearTagScanThreshold"/>:
    /// one well below, one at the boundary, one just above, and one well above.
    /// </summary>
    private static readonly int[] Widths = [1, 4, 16, 17, 40];

    private static string[] Tags(int count, string prefix = "t")
    {
        var tags = new string[count];
        for (var i = 0; i < count; i++)
        {
            tags[i] = prefix + i.ToString();
        }

        return tags;
    }

    [Test]
    public void Adds_every_desired_tag_when_nothing_is_currently_present()
    {
        foreach (var width in Widths)
        {
            var desired = Tags(width);

            LatticeTagIndexContext.ReconcileTagSet(desired, [], out var toAdd, out var toRemove);

            Assert.That(toRemove, Is.Null, $"width {width}: nothing to remove");
            Assert.That(toAdd, Is.Not.Null, $"width {width}: every tag is new");
            Assert.That(toAdd, Is.EqualTo(desired), $"width {width}: first-seen order preserved");
        }
    }

    [Test]
    public void Removes_every_current_tag_when_none_is_desired()
    {
        foreach (var width in Widths)
        {
            var current = Tags(width);

            LatticeTagIndexContext.ReconcileTagSet([], current, out var toAdd, out var toRemove);

            Assert.That(toAdd, Is.Null, $"width {width}: nothing to add");
            Assert.That(toRemove, Is.EqualTo(current), $"width {width}: current order preserved");
        }
    }

    [Test]
    public void Converged_tag_set_allocates_neither_partition()
    {
        foreach (var width in Widths)
        {
            var tags = Tags(width);

            LatticeTagIndexContext.ReconcileTagSet(tags, tags, out var toAdd, out var toRemove);

            Assert.That(toAdd, Is.Null, $"width {width}: no additions when converged");
            Assert.That(toRemove, Is.Null, $"width {width}: no removals when converged");
        }
    }

    [Test]
    public void Partitions_a_partial_overlap_on_both_sides_of_the_threshold()
    {
        foreach (var width in Widths)
        {
            var desired = Tags(width);
            // Keep the first half, drop the second, and introduce one tag that
            // is present but not wanted.
            var keep = width / 2;
            var current = new List<string>(Tags(keep));
            current.Add("stale");

            LatticeTagIndexContext.ReconcileTagSet(desired, current, out var toAdd, out var toRemove);

            Assert.That(toAdd, Is.EqualTo(desired[keep..]), $"width {width}: only the unseen tags are added");
            Assert.That(toRemove, Is.EqualTo(new[] { "stale" }), $"width {width}: only the unwanted row is removed");
        }
    }

    [Test]
    public void Duplicate_desired_tags_collapse_to_one_addition()
    {
        foreach (var width in Widths)
        {
            // Every tag repeated once, so the deduplicated width is `width` but
            // the input width is twice that - crossing the threshold for the
            // smaller cases as well.
            var distinct = Tags(width);
            var withDuplicates = new List<string>(width * 2);
            foreach (var tag in distinct)
            {
                withDuplicates.Add(tag);
                withDuplicates.Add(tag);
            }

            LatticeTagIndexContext.ReconcileTagSet(withDuplicates, [], out var toAdd, out var toRemove);

            Assert.That(toRemove, Is.Null, $"width {width}: nothing to remove");
            Assert.That(toAdd, Is.EqualTo(distinct), $"width {width}: duplicates collapse, first-seen order preserved");
        }
    }

    [Test]
    public void Rejects_an_invalid_tag_on_both_sides_of_the_threshold()
    {
        foreach (var width in Widths)
        {
            var withNul = Tags(width);
            withNul[^1] = "bad\0tag";
            Assert.That(
                () => LatticeTagIndexContext.ReconcileTagSet(withNul, [], out _, out _),
                Throws.ArgumentException,
                $"width {width}: a NUL-bearing tag is rejected");

            var withEmpty = Tags(width);
            withEmpty[^1] = string.Empty;
            Assert.That(
                () => LatticeTagIndexContext.ReconcileTagSet(withEmpty, [], out _, out _),
                Throws.ArgumentException,
                $"width {width}: an empty tag is rejected");
        }
    }

    [Test]
    public void Reconciles_against_a_wide_current_set_that_crosses_the_threshold()
    {
        // `current` wide enough to take the hashed-membership path while
        // `desired` stays narrow, so the two thresholds are exercised
        // independently of one another.
        var current = Tags(LatticeTagIndexContext.LinearTagScanThreshold + 8);
        string[] desired = ["t0", "t1", "fresh"];

        LatticeTagIndexContext.ReconcileTagSet(desired, current, out var toAdd, out var toRemove);

        Assert.That(toAdd, Is.EqualTo(new[] { "fresh" }));
        Assert.That(toRemove, Is.EqualTo(current[2..]));
    }
}
