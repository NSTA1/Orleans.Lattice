namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="RepoContextVectorPayload"/>,
/// the optional-vector value type carried through the portability enumeration and
/// snapshot surface. Its <see cref="RepoContextVectorPayload.Vector"/> byte array
/// was compared by reference under the compiler-generated record-struct equality,
/// so two payloads carrying structurally identical vector bytes never compared
/// equal - inconsistent with the sibling <see cref="RepoContextSnapshotRecord"/>,
/// whose opaque byte payloads are compared by content.
/// </summary>
[TestFixture]
public sealed class RepoContextVectorPayloadEqualityTests
{
    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = new RepoContextVectorPayload([1, 2, 3], "onyx-v1");
        var b = new RepoContextVectorPayload([1, 2, 3], "onyx-v1");

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Vector, b.Vector), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_vector_bytes_differ()
    {
        var a = new RepoContextVectorPayload([1, 2, 3], "onyx-v1");
        var b = new RepoContextVectorPayload([1, 2, 4], "onyx-v1");

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.False);
            Assert.That(a != b, Is.True);
        });
    }

    [Test]
    public void Not_equal_when_vector_lengths_differ()
    {
        var a = new RepoContextVectorPayload([1, 2, 3], "onyx-v1");
        var b = new RepoContextVectorPayload([1, 2], "onyx-v1");

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_embedding_space_differs()
    {
        var a = new RepoContextVectorPayload([1, 2, 3], "onyx-v1");

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(new RepoContextVectorPayload([1, 2, 3], "onyx-v2")), Is.False);
            Assert.That(a.Equals(new RepoContextVectorPayload([1, 2, 3], null)), Is.False);
        });
    }

    [Test]
    public void Equal_when_embedding_space_null_on_both_sides()
    {
        var a = new RepoContextVectorPayload([1, 2, 3], null);
        var b = new RepoContextVectorPayload([1, 2, 3], null);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Default_payloads_are_equal()
    {
        RepoContextVectorPayload a = default;
        RepoContextVectorPayload b = default;

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
