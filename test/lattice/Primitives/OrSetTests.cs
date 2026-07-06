using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrSetTests
{
    private static readonly byte[] Apple = "apple"u8.ToArray();
    private static readonly byte[] Banana = "banana"u8.ToArray();

    [Test]
    public void New_set_is_empty()
    {
        var set = new OrSet();
        Assert.That(set.IsEmpty, Is.True);
        Assert.That(set.Count, Is.EqualTo(0));
        Assert.That(set.Elements(), Is.Empty);
    }

    [Test]
    public void Add_makes_element_a_member()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        Assert.That(set.Contains(Apple), Is.True);
        Assert.That(set.Count, Is.EqualTo(1));
        Assert.That(set.IsEmpty, Is.False);
    }

    [Test]
    public void Remove_drops_observed_dots()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        var removed = set.Remove(Apple);
        Assert.That(removed, Is.True);
        Assert.That(set.Contains(Apple), Is.False);
        Assert.That(set.Count, Is.EqualTo(0));
    }

    [Test]
    public void Remove_returns_false_when_element_absent()
    {
        var set = new OrSet();
        Assert.That(set.Remove(Apple), Is.False);
    }

    [Test]
    public void Remove_returns_false_when_element_already_removed()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Remove(Apple);
        Assert.That(set.Remove(Apple), Is.False);
    }

    [Test]
    public void Add_throws_on_null_element()
    {
        var set = new OrSet();
        Assert.That(() => set.Add(null!, "r1", 1), Throws.ArgumentNullException);
    }

    [Test]
    public void Add_throws_on_empty_replica_id()
    {
        var set = new OrSet();
        Assert.That(() => set.Add(Apple, "", 1), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => set.Add(Apple, null!, 1), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Remove_throws_on_null_element()
    {
        var set = new OrSet();
        Assert.That(() => set.Remove(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Contains_throws_on_null_element()
    {
        var set = new OrSet();
        Assert.That(() => set.Contains(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Empty_element_is_a_valid_member()
    {
        // Empty arrays base64-encode to the empty string; the span-keyed
        // lookup must round-trip through the zero-length key.
        var set = new OrSet();
        var empty = Array.Empty<byte>();
        set.Add(empty, "r1", 1);
        Assert.That(set.Contains(empty), Is.True);
        Assert.That(set.Elements().Single(), Is.Empty);
        Assert.That(set.Remove(empty), Is.True);
        Assert.That(set.Contains(empty), Is.False);
    }

    [TestCase(1)]
    [TestCase(64)]
    [TestCase(192)]     // largest element that keys through the stack buffer
    [TestCase(193)]     // first element that spills to the pooled buffer
    [TestCase(4096)]    // well past the stack-buffer threshold
    public void Elements_of_varying_size_round_trip_through_span_keying(int size)
    {
        // Exercises both the stack-buffer and ArrayPool base64 encode paths
        // in Add / Contains / Remove for elements above and below the
        // MaxStackBase64Chars (256-char => 192-byte) crossover.
        var set = new OrSet();
        var element = new byte[size];
        for (var i = 0; i < size; i++) element[i] = (byte)(i * 31 + 7);

        set.Add(element, "r1", 1);
        Assert.That(set.Contains(element), Is.True);
        Assert.That(set.Count, Is.EqualTo(1));

        var read = set.Elements().Single();
        Assert.That(read, Is.EqualTo(element));

        Assert.That(set.Remove(element), Is.True);
        Assert.That(set.Contains(element), Is.False);
        Assert.That(set.IsEmpty, Is.True);
    }

    [Test]
    public void Re_adding_existing_element_appends_a_second_dot()
    {
        // The re-add hits the span lookup (no new key materialised) and
        // must attach a second concurrent dot to the same element.
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Add(Apple, "r2", 1);
        Assert.That(set.Contains(Apple), Is.True);
        Assert.That(set.Adds, Has.Count.EqualTo(1));
        Assert.That(set.Adds.Values.Single(), Has.Count.EqualTo(2));
    }

    [Test]
    public void Concurrent_add_survives_remove_on_other_replica()
    {
        // r1 and r2 each add "apple" with their own dot.
        var r1 = new OrSet();
        r1.Add(Apple, "r1", 1);

        var r2 = new OrSet();
        r2.Add(Apple, "r2", 1);

        // r1 then removes "apple" (only sees its own dot).
        r1.Remove(Apple);

        // Merging both: r2's dot is not tombstoned, so apple survives.
        var merged = OrSet.Merge(r1, r2);
        Assert.That(merged.Contains(Apple), Is.True);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new OrSet();
        a.Add(Apple, "r1", 1);
        var b = new OrSet();
        b.Add(Banana, "r2", 1);

        var ab = OrSet.Merge(a, b);
        var ba = OrSet.Merge(b, a);

        Assert.That(ab.Contains(Apple), Is.True);
        Assert.That(ab.Contains(Banana), Is.True);
        Assert.That(ba.Contains(Apple), Is.True);
        Assert.That(ba.Contains(Banana), Is.True);
        Assert.That(ab.Count, Is.EqualTo(ba.Count));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new OrSet();
        a.Add(Apple, "r1", 1);
        a.Add(Banana, "r1", 2);
        var twice = OrSet.Merge(a, a);
        Assert.That(twice.Count, Is.EqualTo(2));
        Assert.That(twice.Contains(Apple), Is.True);
        Assert.That(twice.Contains(Banana), Is.True);
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new OrSet(); a.Add(Apple, "r1", 1);
        var b = new OrSet(); b.Add(Banana, "r2", 1);
        var c = new OrSet(); c.Add(Apple, "r3", 1);

        var left = OrSet.Merge(OrSet.Merge(a, b), c);
        var right = OrSet.Merge(a, OrSet.Merge(b, c));
        Assert.That(left.Count, Is.EqualTo(right.Count));
        Assert.That(left.Contains(Apple), Is.EqualTo(right.Contains(Apple)));
        Assert.That(left.Contains(Banana), Is.EqualTo(right.Contains(Banana)));
    }

    [Test]
    public void Merge_throws_on_null()
    {
        var a = new OrSet();
        Assert.That(() => OrSet.Merge(null!, a), Throws.ArgumentNullException);
        Assert.That(() => OrSet.Merge(a, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_deep_copy()
    {
        var a = new OrSet();
        a.Add(Apple, "r1", 1);
        var copy = a.Clone();
        copy.Add(Banana, "r1", 2);

        Assert.That(a.Contains(Banana), Is.False);
        Assert.That(copy.Contains(Banana), Is.True);
    }

    [Test]
    public void Elements_enumerates_only_live_members_in_ordinal_order()
    {
        var set = new OrSet();
        set.Add(Banana, "r1", 1);
        set.Add(Apple, "r1", 2);
        set.Add("cherry"u8.ToArray(), "r1", 3);
        set.Remove(Banana);

        var live = set.Elements().Select(System.Text.Encoding.UTF8.GetString).ToHashSet();
        Assert.That(live, Is.EquivalentTo(new[] { "apple", "cherry" }));
    }

    [Test]
    public void Re_add_after_remove_makes_element_a_member_again()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Remove(Apple);
        set.Add(Apple, "r1", 2);
        Assert.That(set.Contains(Apple), Is.True);
    }

    [Test]
    public void Empty_array_is_a_valid_distinct_element()
    {
        var set = new OrSet();
        set.Add([], "r1", 1);
        Assert.That(set.Contains([]), Is.True);
        Assert.That(set.Contains(Apple), Is.False);
    }

    [Test]
    public void Merge_throws_on_null_left()
    {
        var s = new OrSet();
        Assert.That(() => OrSet.Merge(null!, s), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Merge_throws_on_null_right()
    {
        var s = new OrSet();
        Assert.That(() => OrSet.Merge(s, null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeFrom_throws_on_null_other()
    {
        var s = new OrSet();
        Assert.That(() => s.MergeFrom(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeFrom_matches_static_Merge()
    {
        var a = new OrSet();
        a.Add(Apple, "r1", 1);
        a.Add(Banana, "r1", 2);

        var b = new OrSet();
        b.Add(Apple, "r2", 1);
        b.Remove(Banana);
        b.Add(Banana, "r2", 5);

        var staticResult = OrSet.Merge(a, b);
        var inPlace = a.Clone();
        inPlace.MergeFrom(b);

        var staticElements = staticResult.Elements().Select(Convert.ToBase64String).OrderBy(s => s, StringComparer.Ordinal).ToList();
        var inPlaceElements = inPlace.Elements().Select(Convert.ToBase64String).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.That(inPlaceElements, Is.EqualTo(staticElements));
        Assert.That(inPlace.Count, Is.EqualTo(staticResult.Count));
    }

    [Test]
    public void MergeMap_dedups_many_dots_per_element_and_remains_idempotent()
    {
        // Stress the new HashSet-backed MergeMap dedup: 50 dots per side
        // for the same element, with 25 overlapping dots.
        var a = new OrSet();
        var b = new OrSet();
        for (var i = 1; i <= 50; i++) a.Add(Apple, "r1", i);
        for (var i = 26; i <= 75; i++) b.Add(Apple, "r1", i);

        var ab = OrSet.Merge(a, b);
        var ba = OrSet.Merge(b, a);
        var abab = OrSet.Merge(ab, a); // idempotent re-merge with operand

        var keyB64 = Convert.ToBase64String(Apple);
        // Expected: union of {1..50} ∪ {26..75} = {1..75} = 75 unique dots.
        Assert.That(ab.Adds[keyB64].Count, Is.EqualTo(75));
        // Commutative: same dot set regardless of order.
        Assert.That(ba.Adds[keyB64].Count, Is.EqualTo(75));
        // Idempotent: re-merging with one operand does not duplicate.
        Assert.That(abab.Adds[keyB64].Count, Is.EqualTo(75));
        Assert.That(ab.Contains(Apple), Is.True);
    }
}
