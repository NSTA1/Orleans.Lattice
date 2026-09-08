namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="LatticeSchemaRemediationOutcome"/>,
/// the pure in-process dry-run result. Its
/// <see cref="LatticeSchemaRemediationOutcome.OffendingValuePreview"/> byte array was
/// compared by reference under the compiler-generated record-struct equality, so two
/// structurally identical aborted outcomes never compared equal.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeSchemaRemediationOutcomeEqualityTests
{
    private static LatticeSchemaRemediationOutcome Aborted(byte[] preview) =>
        LatticeSchemaRemediationOutcome.Aborted(
            scannedCount: 3, offendingKey: "k", reason: "bad", offendingValuePreview: preview);

    [Test]
    public void Equal_when_preview_bytes_match_across_distinct_arrays()
    {
        var a = Aborted([1, 2, 3]);
        var b = Aborted([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.OffendingValuePreview, b.OffendingValuePreview), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_preview_bytes_differ()
    {
        Assert.That(Aborted([1, 2, 3]).Equals(Aborted([1, 2, 4])), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Aborted([1, 2, 3]).Equals(LatticeSchemaRemediationOutcome.Aborted(3, "other", "bad", [1, 2, 3])),
                Is.False);
            Assert.That(
                Aborted([1, 2, 3]).Equals(LatticeSchemaRemediationOutcome.Aborted(3, "k", "other", [1, 2, 3])),
                Is.False);
            Assert.That(
                Aborted([1, 2, 3]).Equals(LatticeSchemaRemediationOutcome.Aborted(9, "k", "bad", [1, 2, 3])),
                Is.False);
        });
    }

    [Test]
    public void Success_outcomes_with_null_preview_compare_equal()
    {
        var a = LatticeSchemaRemediationOutcome.Success(5);
        var b = LatticeSchemaRemediationOutcome.Success(5);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_one_preview_is_null()
    {
        var withPreview = Aborted([1, 2, 3]);
        var withoutPreview = LatticeSchemaRemediationOutcome.Aborted(3, "k", "bad", null!);

        Assert.That(withPreview.Equals(withoutPreview), Is.False);
    }

    [Test]
    public void Success_and_abort_do_not_compare_equal()
    {
        Assert.That(LatticeSchemaRemediationOutcome.Success(3).Equals(Aborted([1])), Is.False);
    }
}
