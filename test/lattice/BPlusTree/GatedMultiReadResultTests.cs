using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="GatedMultiReadResult"/>, the additive multi-read
/// result that carries the read-path access gate's prune count alongside the
/// returned rows (issue #2277).
/// </summary>
[TestFixture]
public sealed class GatedMultiReadResultTests
{
    private static byte[] Bytes(params byte[] value) => value;

    [Test]
    public void Default_instance_has_no_values_and_prunes_nothing()
    {
        var result = new GatedMultiReadResult();

        Assert.Multiple(() =>
        {
            Assert.That(result.Values, Is.Empty, "Values defaults to an empty dictionary, never null.");
            Assert.That(result.PrunedByAccessGate, Is.Zero);
            Assert.That(result.IsComplete, Is.True, "Nothing pruned means absence is interpretable.");
        });
    }

    [Test]
    public void IsComplete_is_false_once_the_gate_has_pruned_anything()
    {
        var result = new GatedMultiReadResult { PrunedByAccessGate = 1 };

        Assert.That(result.IsComplete, Is.False);
    }

    [Test]
    public void IsComplete_tracks_the_prune_count_and_not_the_row_count()
    {
        // An empty read is complete when nothing was pruned - a repository with no
        // matching keys is a genuine, interpretable absence - and a full read is
        // incomplete when something was. The two counts are independent, which is
        // the whole reason the prune count has to be reported separately.
        var emptyButUngated = new GatedMultiReadResult();
        var populatedButGated = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1) },
            PrunedByAccessGate = 2,
        };

        Assert.Multiple(() =>
        {
            Assert.That(emptyButUngated.IsComplete, Is.True);
            Assert.That(populatedButGated.IsComplete, Is.False);
        });
    }

    [Test]
    public void Equals_compares_row_values_by_content_rather_than_by_reference()
    {
        // The compiler-generated record equality would compare the dictionaries with
        // EqualityComparer<Dictionary<,>>.Default, which is reference equality, so
        // two structurally identical results built independently would compare
        // unequal - and so would a result and its post-serialization self.
        var left = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1, 2) },
        };
        var right = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1, 2) },
        };

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Equals_is_false_when_a_row_value_differs()
    {
        var left = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1, 2) },
        };
        var right = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1, 3) },
        };

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Equals_is_false_when_a_row_key_differs()
    {
        var left = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1) },
        };
        var right = new GatedMultiReadResult
        {
            Values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["b"] = Bytes(1) },
        };

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Equals_is_false_when_only_the_prune_count_differs()
    {
        // The distinguishing field must participate in equality, or a round-trip
        // test could pass while silently dropping the very number this type exists
        // to carry.
        var values = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["a"] = Bytes(1) };
        var ungated = new GatedMultiReadResult { Values = values };
        var gated = new GatedMultiReadResult { Values = values, PrunedByAccessGate = 1 };

        Assert.That(ungated, Is.Not.EqualTo(gated));
    }

    [Test]
    public void Equals_is_false_against_null()
    {
        Assert.That(new GatedMultiReadResult().Equals(null), Is.False);
    }
}
