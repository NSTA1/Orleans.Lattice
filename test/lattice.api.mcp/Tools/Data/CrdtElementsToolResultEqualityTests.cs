using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="CrdtElementsToolResult"/>, the
/// structured result the OR-Set / MV-Register / Sequence read tools return. Its
/// <see cref="CrdtElementsToolResult.Elements"/> byte arrays were compared by
/// reference under the compiler-generated record equality, so two structurally
/// identical results - including a result and its post-serialization self - never
/// compared equal. This mirrors the already-hardened <c>DataGetToolResult</c>.
/// </summary>
[TestFixture]
public sealed class CrdtElementsToolResultEqualityTests
{
    private static readonly JsonSerializerOptions Options = LatticeApiMcpToolSerialization.Options;

    private static CrdtElementsToolResult Sample(IReadOnlyList<byte[]>? elements = null) => new()
    {
        TreeId = "tree",
        Key = "k",
        Elements = elements ?? [[1, 2, 3], [4]],
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([[1, 2, 3], [4]]);
        var b = Sample([[1, 2, 3], [4]]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Elements[0], b.Elements[0]), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_element_bytes_differ()
    {
        var a = Sample();
        var b = a with { Elements = [[1, 2, 3], [9]] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_element_count_differs()
    {
        var a = Sample([[1]]);
        var b = Sample([[1], [2]]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { TreeId = "other" }), Is.False);
            Assert.That(a.Equals(a with { Key = "other" }), Is.False);
        });
    }

    [Test]
    public void Equal_when_elements_empty_on_both_sides()
    {
        var a = Sample([]);
        var b = Sample([]);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var result = Sample();

        var decoded = JsonSerializer.Deserialize<CrdtElementsToolResult>(
            JsonSerializer.Serialize(result, Options), Options);

        Assert.That(decoded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded!.Elements[0], result.Elements[0]), Is.False);
            Assert.That(decoded.Equals(result), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(result.GetHashCode()));
        });
    }
}
