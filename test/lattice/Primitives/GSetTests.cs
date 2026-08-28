using System.Text;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class GSetTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public void New_set_is_empty_and_bottom()
    {
        var set = new GSet();
        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.True);
            Assert.That(set.IsBottom, Is.True);
            Assert.That(set.Count, Is.EqualTo(0));
            Assert.That(set.Elements, Is.Empty);
        });
    }

    [Test]
    public void Add_inserts_new_element_and_reports_true()
    {
        var set = new GSet();
        var added = set.Add(B("a"));
        Assert.Multiple(() =>
        {
            Assert.That(added, Is.True);
            Assert.That(set.Contains(B("a")), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
            Assert.That(set.IsBottom, Is.False);
        });
    }

    [Test]
    public void Add_is_idempotent_and_reports_false_on_duplicate()
    {
        var set = new GSet();
        set.Add(B("a"));
        var second = set.Add(B("a"));
        Assert.Multiple(() =>
        {
            Assert.That(second, Is.False);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Add_treats_elements_with_equal_content_as_equal()
    {
        var set = new GSet();
        set.Add(B("hello"));
        var second = set.Add(Encoding.UTF8.GetBytes("hello"));
        Assert.Multiple(() =>
        {
            Assert.That(second, Is.False);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Add_accepts_empty_array_as_valid_element()
    {
        var set = new GSet();
        var added = set.Add(Array.Empty<byte>());
        Assert.Multiple(() =>
        {
            Assert.That(added, Is.True);
            Assert.That(set.Contains(Array.Empty<byte>()), Is.True);
        });
    }

    [Test]
    public void Add_throws_on_null_element()
        => Assert.That(() => new GSet().Add(null!), Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void Contains_throws_on_null_element()
        => Assert.That(() => new GSet().Contains(null!), Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void Contains_is_false_for_absent_element()
        => Assert.That(new GSet().Contains(B("missing")), Is.False);

    [Test]
    public void Add_of_large_element_uses_pool_path_and_round_trips()
    {
        var set = new GSet();
        var big = new byte[512];
        for (var i = 0; i < big.Length; i++) big[i] = (byte)(i % 251);
        Assert.Multiple(() =>
        {
            Assert.That(set.Add(big), Is.True);
            Assert.That(set.Contains(big), Is.True);
        });
    }

    [Test]
    public void Values_returns_all_elements_in_ordinal_key_order()
    {
        var set = new GSet();
        // Base64 keys sort in a different order from the raw bytes: 0xFF is
        // "/w==", 0x00 is "AA==", 0x34 is "NA==", and '/' (0x2F) sorts before
        // 'A' (0x41) which sorts before 'N' (0x4E). Order-insensitive
        // assertions cannot tell the two orderings apart, so the expectation
        // has to be exact.
        set.Add([0x00]);
        set.Add([0x34]);
        set.Add([0xFF]);

        var values = set.Values().ToList();

        Assert.That(values, Is.EqualTo(new[]
        {
            new byte[] { 0xFF },
            new byte[] { 0x00 },
            new byte[] { 0x34 },
        }));
    }

    [Test]
    public void Values_on_empty_set_is_empty()
        => Assert.That(new GSet().Values(), Is.Empty);

    [Test]
    public void Merge_is_the_union_of_both_sides()
    {
        var left = new GSet();
        left.Add(B("a"));
        left.Add(B("b"));
        var right = new GSet();
        right.Add(B("b"));
        right.Add(B("c"));

        var merged = GSet.Merge(left, right);
        Assert.Multiple(() =>
        {
            Assert.That(merged.Count, Is.EqualTo(3));
            Assert.That(merged.Contains(B("a")), Is.True);
            Assert.That(merged.Contains(B("b")), Is.True);
            Assert.That(merged.Contains(B("c")), Is.True);
        });
    }

    [Test]
    public void Merge_does_not_mutate_the_operands()
    {
        var left = new GSet();
        left.Add(B("a"));
        var right = new GSet();
        right.Add(B("b"));

        _ = GSet.Merge(left, right);
        Assert.Multiple(() =>
        {
            Assert.That(left.Count, Is.EqualTo(1));
            Assert.That(right.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new GSet();
        a.Add(B("x"));
        a.Add(B("y"));
        var b = new GSet();
        b.Add(B("y"));
        b.Add(B("z"));

        var ab = GSet.Merge(a, b);
        var ba = GSet.Merge(b, a);
        Assert.That(ab.Elements, Is.EquivalentTo(ba.Elements));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new GSet(); a.Add(B("1"));
        var b = new GSet(); b.Add(B("2"));
        var c = new GSet(); c.Add(B("3"));

        var leftAssoc = GSet.Merge(GSet.Merge(a, b), c);
        var rightAssoc = GSet.Merge(a, GSet.Merge(b, c));
        Assert.That(leftAssoc.Elements, Is.EquivalentTo(rightAssoc.Elements));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new GSet();
        a.Add(B("a"));
        a.Add(B("b"));

        var once = GSet.Merge(a, a);
        var twice = GSet.Merge(once, a);
        Assert.Multiple(() =>
        {
            Assert.That(once.Elements, Is.EquivalentTo(a.Elements));
            Assert.That(twice.Elements, Is.EquivalentTo(a.Elements));
        });
    }

    [Test]
    public void Merge_throws_on_null_operands()
    {
        var set = new GSet();
        Assert.Multiple(() =>
        {
            Assert.That(() => GSet.Merge(null!, set), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => GSet.Merge(set, null!), Throws.InstanceOf<ArgumentNullException>());
        });
    }

    [Test]
    public void MergeFrom_unions_other_into_this_set()
    {
        var target = new GSet();
        target.Add(B("a"));
        var other = new GSet();
        other.Add(B("b"));

        target.MergeFrom(other);
        Assert.Multiple(() =>
        {
            Assert.That(target.Count, Is.EqualTo(2));
            Assert.That(target.Contains(B("a")), Is.True);
            Assert.That(target.Contains(B("b")), Is.True);
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
        => Assert.That(() => new GSet().MergeFrom(null!), Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void Clone_produces_an_independent_copy()
    {
        var original = new GSet();
        original.Add(B("a"));

        var copy = original.Clone();
        copy.Add(B("b"));

        Assert.Multiple(() =>
        {
            Assert.That(original.Count, Is.EqualTo(1));
            Assert.That(original.Contains(B("b")), Is.False);
            Assert.That(copy.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void MergeDelta_adds_all_delta_elements()
    {
        var set = new GSet();
        set.Add(B("a"));
        var delta = new GSetDelta { Adds = new[] { B("b"), B("c") } };

        set.MergeDelta(delta);
        Assert.Multiple(() =>
        {
            Assert.That(set.Count, Is.EqualTo(3));
            Assert.That(set.Contains(B("b")), Is.True);
            Assert.That(set.Contains(B("c")), Is.True);
        });
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var set = new GSet();
        var delta = new GSetDelta { Adds = new[] { B("a"), B("b") } };

        set.MergeDelta(delta);
        set.MergeDelta(delta);
        Assert.That(set.Count, Is.EqualTo(2));
    }

    [Test]
    public void MergeDelta_with_empty_adds_is_a_noop()
    {
        var set = new GSet();
        set.Add(B("a"));
        set.MergeDelta(GSetDelta.Empty);
        Assert.That(set.Count, Is.EqualTo(1));
    }

    [Test]
    public void MergeDelta_ignores_null_adds_collection()
    {
        var set = new GSet();
        set.Add(B("a"));
        set.MergeDelta(new GSetDelta { Adds = null! });
        Assert.That(set.Count, Is.EqualTo(1));
    }

    [Test]
    public void GSetDelta_Empty_carries_a_non_null_empty_collection()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GSetDelta.Empty.Adds, Is.Not.Null);
            Assert.That(GSetDelta.Empty.Adds, Is.Empty);
        });
    }
}
