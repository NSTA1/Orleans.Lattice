namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Covers <see cref="SynthesizedCollectionTypeNames"/>, the read-side repair for
/// JSON rows that name a compiler-synthesized collection type (issue #5579).
/// </summary>
[TestFixture]
public sealed class SynthesizedCollectionTypeNamesTests
{
    [Test]
    public void Repair_removes_a_synthesized_type_tag_and_keeps_the_values()
    {
        const string json = """{"Record":{"$id":"2","TreeIds":{"$type":"<>z__ReadOnlyArray`1[[string]],Orleans.Lattice","$values":["a","b"]}}}""";

        Assert.That(
            SynthesizedCollectionTypeNames.Repair(json),
            Is.EqualTo("""{"Record":{"$id":"2","TreeIds":{"$values":["a","b"]}}}"""));
    }

    [Test]
    public void Repair_handles_every_synthesized_shape_and_escaped_brackets()
    {
        const string json = """{"A":{"$type":"<>z__ReadOnlySingleElementList`1[[string]],X","$values":["a"]},"B":{"$type":"\u003c\u003ez__ReadOnlyList`1[[string]],X","$values":[]}}""";

        Assert.That(
            SynthesizedCollectionTypeNames.Repair(json),
            Is.EqualTo("""{"A":{"$values":["a"]},"B":{"$values":[]}}"""));
    }

    [Test]
    public void Repair_returns_the_same_instance_when_nothing_matches()
    {
        const string json = """{"TreeIds":{"$type":"System.String[], System.Private.CoreLib","$values":["a"]}}""";

        Assert.That(SynthesizedCollectionTypeNames.Repair(json), Is.SameAs(json));
    }

    [Test]
    public void Repair_never_touches_text_inside_a_string_value()
    {
        const string json = """{"Note":"\"$type\":\"<>z__ReadOnlyArray`1\","}""";

        Assert.That(SynthesizedCollectionTypeNames.Repair(json), Is.SameAs(json));
    }

    [Test]
    public void Repair_rejects_null() =>
        Assert.Throws<ArgumentNullException>(() => SynthesizedCollectionTypeNames.Repair(null!));

    [Test]
    public void MayContain_detects_the_marker_only_when_present()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SynthesizedCollectionTypeNames.MayContain("""{"$type":"<>z__ReadOnlyArray`1"}"""u8), Is.True);
            Assert.That(SynthesizedCollectionTypeNames.MayContain("""{"$type":"System.String[]"}"""u8), Is.False);
        });
    }

    [Test]
    public void A_payload_without_the_marker_reaches_the_fallback_unchanged()
    {
        using var storage = new SiloJsonGrainStorage();
        var payload = storage.Json.Serialize(new List<string> { "a" });

        Assert.That(storage.Lattice.Deserialize<List<string>>(payload), Is.EqualTo(new[] { "a" }));
    }
}
