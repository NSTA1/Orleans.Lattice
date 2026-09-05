using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="DataGetToolResult"/>, the
/// structured result the <c>data_get</c> MCP tool returns. Its
/// <see cref="DataGetToolResult.Value"/> byte array was compared by reference under
/// the compiler-generated record equality, so two structurally identical results -
/// including a result and its post-serialization self - never compared equal. This
/// mirrors the already-hardened facade record <c>DataReadResult</c>.
/// </summary>
[TestFixture]
public sealed class DataGetToolResultEqualityTests
{
    private static readonly JsonSerializerOptions Options = LatticeApiMcpToolSerialization.Options;

    private static DataGetToolResult Sample(byte[]? value = null) => new()
    {
        TreeId = "tree",
        Key = "k",
        Found = true,
        Value = value ?? [1, 2, 3],
        MergeMode = "OrSet",
        Raw = true,
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample();
        var b = a with { Value = [9, 9] };

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
            Assert.That(a.Equals(a with { Found = false }), Is.False);
            Assert.That(a.Equals(a with { MergeMode = "PnCounter" }), Is.False);
            Assert.That(a.Equals(a with { Raw = false }), Is.False);
        });
    }

    [Test]
    public void Equal_when_values_empty_on_both_sides()
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

        var decoded = JsonSerializer.Deserialize<DataGetToolResult>(
            JsonSerializer.Serialize(result, Options), Options);

        Assert.That(decoded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded!.Value, result.Value), Is.False);
            Assert.That(decoded.Equals(result), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(result.GetHashCode()));
        });
    }
}
