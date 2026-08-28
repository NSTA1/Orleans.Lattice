namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ChangeFeedCursor"/>, the public per-partition
/// offset cursor threaded through <c>IChangeFeed.Subscribe</c>. The type is a
/// defensive snapshot with structural equality, so these tests pin the three
/// properties a consumer relies on: a caller cannot mutate a cursor after
/// handing its dictionary over, a negative offset is rejected at construction
/// rather than silently skipping entries, and equality/hashing are structural
/// and iteration-order independent (so a cursor can be used as a dictionary key
/// or compared across a persistence round trip).
/// </summary>
[TestFixture]
public sealed class ChangeFeedCursorTests
{
    [Test]
    public void Initial_yields_offset_zero_for_every_partition()
    {
        var cursor = ChangeFeedCursor.Initial;

        Assert.Multiple(() =>
        {
            Assert.That(cursor.GetOffsetForPartition(0), Is.Zero);
            Assert.That(cursor.GetOffsetForPartition(7), Is.Zero);
            Assert.That(cursor.PartitionOffsets, Is.Empty);
        });
    }

    [Test]
    public void Default_is_equivalent_to_Initial()
        => Assert.That(default(ChangeFeedCursor), Is.EqualTo(ChangeFeedCursor.Initial));

    [Test]
    public void Constructing_from_null_is_equivalent_to_Initial()
    {
        var cursor = new ChangeFeedCursor(null);

        Assert.Multiple(() =>
        {
            Assert.That(cursor, Is.EqualTo(ChangeFeedCursor.Initial));
            Assert.That(cursor.PartitionOffsets, Is.Empty);
        });
    }

    [Test]
    public void Constructing_from_an_empty_map_is_equivalent_to_Initial()
    {
        var cursor = new ChangeFeedCursor(new Dictionary<int, long>());

        Assert.Multiple(() =>
        {
            Assert.That(cursor, Is.EqualTo(ChangeFeedCursor.Initial));
            Assert.That(cursor.PartitionOffsets, Is.Empty);
        });
    }

    [Test]
    public void GetOffsetForPartition_returns_the_stored_offset_and_zero_for_absent_partitions()
    {
        var cursor = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 12, [3] = 40 });

        Assert.Multiple(() =>
        {
            Assert.That(cursor.GetOffsetForPartition(0), Is.EqualTo(12L));
            Assert.That(cursor.GetOffsetForPartition(3), Is.EqualTo(40L));
            Assert.That(cursor.GetOffsetForPartition(1), Is.Zero);
        });
    }

    [Test]
    public void PartitionOffsets_exposes_the_stored_snapshot()
    {
        var cursor = new ChangeFeedCursor(new Dictionary<int, long> { [2] = 5 });

        Assert.That(cursor.PartitionOffsets, Is.EquivalentTo(new Dictionary<int, long> { [2] = 5 }));
    }

    [Test]
    public void Constructor_defensively_snapshots_the_supplied_map()
    {
        var source = new Dictionary<int, long> { [0] = 10 };
        var cursor = new ChangeFeedCursor(source);

        // Caller-side mutation after construction must not poison the cursor:
        // a subscriber would otherwise silently resume from a different offset.
        source[0] = 999;
        source[1] = 999;

        Assert.Multiple(() =>
        {
            Assert.That(cursor.GetOffsetForPartition(0), Is.EqualTo(10L));
            Assert.That(cursor.GetOffsetForPartition(1), Is.Zero);
        });
    }

    [Test]
    public void Constructor_accepts_a_zero_offset()
        => Assert.That(new ChangeFeedCursor(new Dictionary<int, long> { [1] = 0 }).GetOffsetForPartition(1), Is.Zero);

    [Test]
    public void Constructor_rejects_a_negative_offset()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => _ = new ChangeFeedCursor(new Dictionary<int, long> { [4] = -1 }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("partitionOffsets"));
            Assert.That(ex.Message, Does.Contain("Partition 4"));
        });
    }

    [Test]
    public void Cursors_with_the_same_partition_map_are_equal()
    {
        var a = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1, [1] = 2 });
        // Built in the opposite insertion order so equality cannot depend on it.
        var b = new ChangeFeedCursor(new Dictionary<int, long> { [1] = 2, [0] = 1 });

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a == b, Is.True);
            Assert.That(a != b, Is.False);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Cursors_differing_in_an_offset_are_not_equal()
    {
        var a = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 });
        var b = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.Not.EqualTo(b));
            Assert.That(a == b, Is.False);
            Assert.That(a != b, Is.True);
        });
    }

    [Test]
    public void Cursors_differing_in_a_partition_key_are_not_equal()
    {
        var a = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 });
        var b = new ChangeFeedCursor(new Dictionary<int, long> { [1] = 1 });

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Cursors_differing_in_partition_count_are_not_equal()
    {
        var a = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 });
        var b = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1, [1] = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.Not.EqualTo(b));
            Assert.That(b, Is.Not.EqualTo(a));
        });
    }

    [Test]
    public void A_populated_cursor_is_not_equal_to_Initial_in_either_direction()
    {
        var populated = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 });

        Assert.Multiple(() =>
        {
            Assert.That(populated, Is.Not.EqualTo(ChangeFeedCursor.Initial));
            Assert.That(ChangeFeedCursor.Initial, Is.Not.EqualTo(populated));
        });
    }

    [Test]
    public void Equals_object_matches_only_another_cursor()
    {
        var cursor = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 });

        Assert.Multiple(() =>
        {
            Assert.That(cursor.Equals((object)new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 })), Is.True);
            Assert.That(cursor.Equals((object?)null), Is.False);
            Assert.That(cursor.Equals("not a cursor"), Is.False);
        });
    }

    [Test]
    public void GetHashCode_of_Initial_is_zero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ChangeFeedCursor.Initial.GetHashCode(), Is.Zero);
            Assert.That(new ChangeFeedCursor(new Dictionary<int, long>()).GetHashCode(), Is.Zero);
        });
    }

    [Test]
    public void GetHashCode_is_stable_for_the_same_cursor()
    {
        var cursor = new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1, [9] = 44 });

        Assert.That(cursor.GetHashCode(), Is.EqualTo(cursor.GetHashCode()));
    }

    [Test]
    public void A_cursor_is_usable_as_a_dictionary_key()
    {
        var map = new Dictionary<ChangeFeedCursor, string>
        {
            [new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 })] = "first",
        };

        Assert.That(map[new ChangeFeedCursor(new Dictionary<int, long> { [0] = 1 })], Is.EqualTo("first"));
    }
}
