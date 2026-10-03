namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Covers <see cref="CrdtDeltaListEquality"/>, the element-wise equality and
/// hashing the CRDT delta records apply to their collections.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class CrdtDeltaListEqualityTests
{
    private static readonly OrSetDot A = new() { ReplicaId = "r1", Counter = 1 };
    private static readonly OrSetDot B = new() { ReplicaId = "r2", Counter = 2 };

    [Test]
    public void ListEqual_compares_independently_allocated_lists_by_element()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>([A, B], new List<OrSetDot> { A, B }), Is.True);
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>([A, B], [B, A]), Is.False, "order matters");
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>([A], [A, B]), Is.False);
        });
    }

    [Test]
    public void ListEqual_treats_null_as_distinct_from_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>(null, null), Is.True);
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>(null, []), Is.False);
            Assert.That(CrdtDeltaListEquality.ListEqual<OrSetDot>([], null), Is.False);
        });
    }

    [Test]
    public void AddList_hashes_equal_lists_equally()
    {
        Assert.That(HashOf([A, B]), Is.EqualTo(HashOf(new List<OrSetDot> { A, B })));
    }

    [Test]
    public void AddList_distinguishes_a_null_list_from_a_list_of_one_element()
    {
        Assert.That(HashOf(null), Is.Not.EqualTo(HashOf([A])));
    }

    private static int HashOf(IReadOnlyList<OrSetDot>? list)
    {
        var hash = new HashCode();
        CrdtDeltaListEquality.AddList(ref hash, list);
        return hash.ToHashCode();
    }
}
