using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaValueChecks"/>: strict UTF-8 validity, JSON
/// well-formedness, and JSON string-member projection.
/// </summary>
public class SchemaValueChecksTests
{
    [Test]
    public void IsValidUtf8_well_formed_text_returns_true()
    {
        Assert.That(SchemaValueChecks.IsValidUtf8(Encoding.UTF8.GetBytes("hello world")), Is.True);
    }

    [Test]
    public void IsValidUtf8_empty_returns_true()
    {
        Assert.That(SchemaValueChecks.IsValidUtf8(Array.Empty<byte>()), Is.True);
    }

    [Test]
    public void IsValidUtf8_invalid_byte_sequence_returns_false()
    {
        // 0xC3 0x28 is an invalid two-byte sequence (0x28 is not a continuation byte).
        Assert.That(SchemaValueChecks.IsValidUtf8(new byte[] { 0xC3, 0x28 }), Is.False);
    }

    [Test]
    public void IsWellFormedJson_object_returns_true()
    {
        Assert.That(SchemaValueChecks.IsWellFormedJson(Encoding.UTF8.GetBytes("{\"a\":1}")), Is.True);
    }

    [Test]
    public void IsWellFormedJson_scalar_returns_true()
    {
        Assert.That(SchemaValueChecks.IsWellFormedJson(Encoding.UTF8.GetBytes("42")), Is.True);
    }

    [Test]
    public void IsWellFormedJson_empty_returns_false()
    {
        Assert.That(SchemaValueChecks.IsWellFormedJson(Array.Empty<byte>()), Is.False);
    }

    [Test]
    public void IsWellFormedJson_malformed_returns_false()
    {
        Assert.That(SchemaValueChecks.IsWellFormedJson(Encoding.UTF8.GetBytes("{\"a\":")), Is.False);
    }

    [Test]
    public void TryProjectStringMember_resolves_top_level_string()
    {
        var value = Encoding.UTF8.GetBytes("{\"name\":\"alice\"}");
        Assert.That(SchemaValueChecks.TryProjectStringMember(value, "name"), Is.EqualTo("alice"));
    }

    [Test]
    public void TryProjectStringMember_resolves_nested_string_case_insensitively()
    {
        var value = Encoding.UTF8.GetBytes("{\"Outer\":{\"Inner\":\"deep\"}}");
        Assert.That(SchemaValueChecks.TryProjectStringMember(value, "outer.inner"), Is.EqualTo("deep"));
    }

    [Test]
    public void TryProjectStringMember_missing_path_returns_null()
    {
        var value = Encoding.UTF8.GetBytes("{\"name\":\"alice\"}");
        Assert.That(SchemaValueChecks.TryProjectStringMember(value, "age"), Is.Null);
    }

    [Test]
    public void TryProjectStringMember_non_string_member_returns_null()
    {
        var value = Encoding.UTF8.GetBytes("{\"age\":30}");
        Assert.That(SchemaValueChecks.TryProjectStringMember(value, "age"), Is.Null);
    }

    [Test]
    public void TryProjectStringMember_non_json_returns_null()
    {
        Assert.That(SchemaValueChecks.TryProjectStringMember(new byte[] { 0xC3, 0x28 }, "name"), Is.Null);
    }

    [Test]
    public void TryProjectStringMember_empty_value_returns_null()
    {
        Assert.That(SchemaValueChecks.TryProjectStringMember(Array.Empty<byte>(), "name"), Is.Null);
    }
}
