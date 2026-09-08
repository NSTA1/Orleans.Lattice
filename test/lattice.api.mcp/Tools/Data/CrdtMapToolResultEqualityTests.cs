using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="CrdtMapToolResult"/>, the
/// structured result the OR-Map read tool returns. Its
/// <see cref="CrdtMapToolResult.Fields"/> dictionary of byte-array value lists was
/// compared by reference under the compiler-generated record equality, so two
/// structurally identical results - including a result and its post-serialization
/// self - never compared equal. This mirrors the already-hardened
/// <c>CrdtElementsToolResult</c>.
/// </summary>
[TestFixture]
public sealed class CrdtMapToolResultEqualityTests
{
    private static readonly JsonSerializerOptions Options = LatticeApiMcpToolSerialization.Options;

    private static CrdtMapToolResult Sample(IReadOnlyDictionary<string, IReadOnlyList<byte[]>>? fields = null) => new()
    {
        TreeId = "tree",
        Key = "k",
        Fields = fields ?? new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["a"] = [[1, 2, 3], [4]],
            ["b"] = [[5]],
        },
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Fields["a"][0], b.Fields["a"][0]), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_regardless_of_field_order()
    {
        var a = Sample(new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["a"] = [[1, 2, 3], [4]],
            ["b"] = [[5]],
        });
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["b"] = [[5]],
            ["a"] = [[1, 2, 3], [4]],
        });

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_a_field_value_differs()
    {
        var a = Sample();
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["a"] = [[1, 2, 3], [4]],
            ["b"] = [[9]],
        });

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_field_set_differs()
    {
        var a = Sample();
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["a"] = [[1, 2, 3], [4]],
            ["c"] = [[5]],
        });

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_field_count_differs()
    {
        var a = Sample();
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>>
        {
            ["a"] = [[1, 2, 3], [4]],
        });

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_value_list_length_differs()
    {
        var a = Sample(new Dictionary<string, IReadOnlyList<byte[]>> { ["a"] = [[1]] });
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>> { ["a"] = [[1], [2]] });

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
    public void Equal_when_fields_empty_on_both_sides()
    {
        var a = Sample(new Dictionary<string, IReadOnlyList<byte[]>>());
        var b = Sample(new Dictionary<string, IReadOnlyList<byte[]>>());

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

        var decoded = JsonSerializer.Deserialize<CrdtMapToolResult>(
            JsonSerializer.Serialize(result, Options), Options);

        Assert.That(decoded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded!.Fields["a"][0], result.Fields["a"][0]), Is.False);
            Assert.That(decoded.Equals(result), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(result.GetHashCode()));
        });
    }
}
