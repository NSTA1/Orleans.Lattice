using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="SplitResult.Forward"/> and
/// <see cref="SplitResult.Combine"/>, which let one leaf call return every
/// split it produced rather than only the last one (issue #3523).
/// </summary>
public class SplitResultTests
{
    private static SplitResult Split(string key) => new()
    {
        PromotedKey = key,
        NewSiblingId = GrainId.Create("leaf", key),
        ChildIsLeaf = true,
    };

    [Test]
    public void Forward_null_returns_null()
    {
        Assert.That(SplitResult.Forward(null), Is.Null);
    }

    [Test]
    public void Forward_marks_result_forwarded_and_preserves_fields()
    {
        var own = Split("m");

        var forwarded = SplitResult.Forward(own)!;

        Assert.Multiple(() =>
        {
            Assert.That(forwarded.Forwarded, Is.True);
            Assert.That(forwarded.PromotedKey, Is.EqualTo("m"));
            Assert.That(forwarded.NewSiblingId, Is.EqualTo(own.NewSiblingId));
            Assert.That(forwarded.ChildIsLeaf, Is.True);
            Assert.That(own.Forwarded, Is.False, "the input must not be mutated");
        });
    }

    [Test]
    public void Forward_already_forwarded_returns_same_instance()
    {
        var forwarded = SplitResult.Forward(Split("m"));

        Assert.That(SplitResult.Forward(forwarded), Is.SameAs(forwarded));
    }

    [Test]
    public void Combine_both_null_returns_null()
    {
        Assert.That(SplitResult.Combine(null, null), Is.Null);
    }

    [Test]
    public void Combine_one_null_returns_the_other_unchanged()
    {
        var a = Split("a");
        var b = SplitResult.Forward(Split("b"));

        Assert.Multiple(() =>
        {
            Assert.That(SplitResult.Combine(a, null), Is.SameAs(a));
            Assert.That(SplitResult.Combine(null, b), Is.SameAs(b));
        });
    }

    [Test]
    public void Combine_two_results_keeps_first_as_primary_and_second_in_additional()
    {
        var own = Split("a");
        var forwarded = SplitResult.Forward(Split("b"));

        var combined = SplitResult.Combine(own, forwarded)!;

        Assert.Multiple(() =>
        {
            Assert.That(combined.PromotedKey, Is.EqualTo("a"));
            Assert.That(combined.Forwarded, Is.False);
            Assert.That(combined.Additional, Has.Length.EqualTo(1));
            Assert.That(combined.Additional![0].PromotedKey, Is.EqualTo("b"));
            Assert.That(combined.Additional[0].Forwarded, Is.False,
                "additional entries are always relinked by re-descent, so they carry no Forwarded flag");
            Assert.That(combined.Additional[0].Additional, Is.Null);
        });
    }

    [Test]
    public void Combine_prefers_a_non_forwarded_primary()
    {
        var forwarded = SplitResult.Forward(Split("b"));
        var own = Split("a");

        var combined = SplitResult.Combine(forwarded, own)!;

        Assert.Multiple(() =>
        {
            Assert.That(combined.PromotedKey, Is.EqualTo("a"),
                "only the called leaf's own split may be linked against the captured ancestor path");
            Assert.That(combined.Forwarded, Is.False);
            Assert.That(combined.Additional!.Select(s => s.PromotedKey), Is.EqualTo(new[] { "b" }));
        });
    }

    [Test]
    public void Combine_two_forwarded_results_keeps_primary_forwarded()
    {
        var combined = SplitResult.Combine(
            SplitResult.Forward(Split("a")),
            SplitResult.Forward(Split("b")))!;

        Assert.Multiple(() =>
        {
            Assert.That(combined.PromotedKey, Is.EqualTo("a"));
            Assert.That(combined.Forwarded, Is.True,
                "a primary that is itself forwarded must still be relinked by re-descent");
            Assert.That(combined.Additional!.Select(s => s.PromotedKey), Is.EqualTo(new[] { "b" }));
        });
    }

    [Test]
    public void Combine_flattens_nested_additional_splits_without_losing_any()
    {
        var left = SplitResult.Combine(Split("a"), SplitResult.Forward(Split("b")));
        var right = SplitResult.Combine(SplitResult.Forward(Split("c")), SplitResult.Forward(Split("d")));

        var combined = SplitResult.Combine(left, right)!;

        Assert.Multiple(() =>
        {
            Assert.That(combined.PromotedKey, Is.EqualTo("a"));
            Assert.That(combined.Forwarded, Is.False);
            Assert.That(combined.Additional!.Select(s => s.PromotedKey), Is.EquivalentTo(new[] { "b", "c", "d" }));
            Assert.That(combined.Additional!.All(s => s.Additional is null && !s.Forwarded), Is.True,
                "every additional entry must be flat and unflagged");
        });
    }
}
