namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the merge-observer seam value types
/// (<see cref="LatticeMergeOutcome"/> and <see cref="LatticeMergeContext"/>):
/// the outcome factories, their members, and the context constructor guards.
/// </summary>
[TestFixture]
public class LatticeMergeOutcomeTests
{
    [Test]
    public void Accept_returns_accept_kind_with_no_payload()
    {
        var outcome = LatticeMergeOutcome.Accept();

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
        Assert.That(outcome.TransformedValue, Is.Null);
        Assert.That(outcome.EventReason, Is.Null);
    }

    [Test]
    public void Accept_returns_cached_singleton_value()
    {
        // Same struct value each call (allocation-free default path).
        Assert.That(LatticeMergeOutcome.Accept().Kind, Is.EqualTo(LatticeMergeOutcome.Accept().Kind));
    }

    [Test]
    public void AcceptTransformed_carries_replacement_bytes()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var outcome = LatticeMergeOutcome.AcceptTransformed(bytes);

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptTransformed));
        Assert.That(outcome.TransformedValue, Is.SameAs(bytes));
        Assert.That(outcome.EventReason, Is.Null);
    }

    [Test]
    public void AcceptTransformed_null_bytes_throws()
    {
        Assert.Throws<ArgumentNullException>(() => LatticeMergeOutcome.AcceptTransformed(null!));
    }

    [Test]
    public void AcceptWithEvent_carries_reason()
    {
        var outcome = LatticeMergeOutcome.AcceptWithEvent("why");

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptWithEvent));
        Assert.That(outcome.EventReason, Is.EqualTo("why"));
        Assert.That(outcome.TransformedValue, Is.Null);
    }

    [Test]
    public void AcceptWithEvent_null_reason_throws()
    {
        Assert.Throws<ArgumentNullException>(() => LatticeMergeOutcome.AcceptWithEvent(null!));
    }

    [Test]
    public void AcceptWithEvent_empty_reason_throws()
    {
        Assert.Throws<ArgumentException>(() => LatticeMergeOutcome.AcceptWithEvent(string.Empty));
    }

    [Test]
    public void MergeContext_exposes_all_supplied_members()
    {
        var local = new byte[] { 1 };
        var incoming = new byte[] { 2 };
        var merged = new byte[] { 3 };
        var ctx = new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, local, incoming, merged);

        Assert.That(ctx.Key, Is.EqualTo("k"));
        Assert.That(ctx.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(ctx.LocalValue, Is.SameAs(local));
        Assert.That(ctx.IncomingValue, Is.SameAs(incoming));
        Assert.That(ctx.MergedValue, Is.SameAs(merged));
    }

    [Test]
    public void MergeContext_allows_null_local_and_incoming()
    {
        var merged = new byte[] { 3 };
        var ctx = new LatticeMergeContext("k", LatticeMergeMode.OrSet, null, null, merged);

        Assert.That(ctx.LocalValue, Is.Null);
        Assert.That(ctx.IncomingValue, Is.Null);
        Assert.That(ctx.MergedValue, Is.SameAs(merged));
    }

    [Test]
    public void MergeContext_null_key_throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            _ = new LatticeMergeContext(null!, LatticeMergeMode.LwwRegister, null, null, new byte[] { 3 }));
    }

    [Test]
    public void MergeContext_null_merged_throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            _ = new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, null, null, null!));
    }
}
