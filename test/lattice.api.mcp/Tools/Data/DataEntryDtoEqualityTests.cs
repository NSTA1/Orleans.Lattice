using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="DataEntryDto"/>, the key/value
/// entry the MCP data tools return on a range read and accept on a write batch. Its
/// <see cref="DataEntryDto.Value"/> byte array was compared by reference under the
/// compiler-generated record equality, so two structurally identical entries -
/// including an entry and its post-serialization self - never compared equal. This
/// mirrors the already-hardened facade record <c>DataEntry</c>.
/// </summary>
[TestFixture]
public sealed class DataEntryDtoEqualityTests
{
    private static readonly JsonSerializerOptions Options = LatticeApiMcpToolSerialization.Options;

    private static DataEntryDto Sample(byte[]? value = null) => new()
    {
        Key = "k",
        Value = value ?? [1, 2, 3],
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
    public void Not_equal_when_key_differs()
    {
        var a = Sample();

        Assert.That(a.Equals(a with { Key = "other" }), Is.False);
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
        var entry = Sample();

        var decoded = JsonSerializer.Deserialize<DataEntryDto>(
            JsonSerializer.Serialize(entry, Options), Options);

        Assert.That(decoded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded!.Value, entry.Value), Is.False);
            Assert.That(decoded.Equals(entry), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(entry.GetHashCode()));
        });
    }
}
