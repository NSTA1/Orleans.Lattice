using System.Collections.Immutable;
using System.Collections.ObjectModel;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Covers <see cref="ReadOnlyDictionaryJsonConverter"/>: read-only and immutable
/// dictionaries written by the JSON grain-storage serializer read back, which
/// Newtonsoft refuses under Orleans' reference-preserving settings (issue #5579).
/// </summary>
[TestFixture]
public sealed class ReadOnlyDictionaryJsonConverterTests
{
    private SiloJsonGrainStorage storage = null!;

    [SetUp]
    public void SetUp() => this.storage = new SiloJsonGrainStorage();

    [TearDown]
    public void TearDown() => this.storage.Dispose();

    [Test]
    public void Every_read_only_dictionary_shape_reads_back_with_its_entries()
    {
        var holder = new Holder
        {
            ReadOnly = new ReadOnlyDictionary<string, long>(new Dictionary<string, long> { ["a"] = 1 }),
            Immutable = ImmutableDictionary<string, long>.Empty.Add("b", 2),
            Sorted = ImmutableSortedDictionary<int, string>.Empty.Add(3, "c"),
            BehindInterface = new ReadOnlyDictionary<string, string>(new Dictionary<string, string> { ["d"] = "4" }),
            MutableBehindInterface = new Dictionary<string, string> { ["e"] = "5" },
            ImmutableBehindInterface = ImmutableDictionary<string, string>.Empty.Add("f", "6"),
        };

        var reloaded = this.storage.Json.Deserialize<Holder>(this.storage.Json.Serialize(holder));

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.ReadOnly, Is.EqualTo(holder.ReadOnly));
            Assert.That(reloaded.Immutable, Is.EqualTo(holder.Immutable));
            Assert.That(reloaded.Sorted, Is.EqualTo(holder.Sorted));
            Assert.That(reloaded.BehindInterface, Is.EqualTo(holder.BehindInterface));
            Assert.That(reloaded.BehindInterface, Is.InstanceOf<ReadOnlyDictionary<string, string>>());
            Assert.That(reloaded.MutableBehindInterface, Is.InstanceOf<Dictionary<string, string>>().And.EqualTo(holder.MutableBehindInterface));
            Assert.That(reloaded.ImmutableBehindInterface, Is.InstanceOf<ImmutableDictionary<string, string>>().And.EqualTo(holder.ImmutableBehindInterface));
            Assert.That(reloaded.Missing, Is.Null);
        });
    }

    [Test]
    public void A_map_shared_by_two_members_reads_back_through_its_reference()
    {
        var shared = new ReadOnlyDictionary<string, string>(new Dictionary<string, string> { ["k"] = "v" });
        var holder = new Holder { BehindInterface = shared, ImmutableBehindInterface = shared };

        var payload = this.storage.Json.Serialize(holder);
        Assert.That(payload.ToString(), Does.Contain("$ref"), "The second member must be written as a reference, or this test proves nothing.");

        var reloaded = this.storage.Json.Deserialize<Holder>(payload);

        Assert.That(reloaded.ImmutableBehindInterface, Is.SameAs(reloaded.BehindInterface));
    }

    [Test]
    public void The_converter_matches_only_dictionaries_newtonsoft_cannot_populate()
    {
        var converter = new ReadOnlyDictionaryJsonConverter();

        Assert.Multiple(() =>
        {
            Assert.That(converter.CanConvert(typeof(IReadOnlyDictionary<string, string>)), Is.True);
            Assert.That(converter.CanConvert(typeof(ReadOnlyDictionary<string, string>)), Is.True);
            Assert.That(converter.CanConvert(typeof(ImmutableDictionary<string, long>)), Is.True);
            Assert.That(converter.CanConvert(typeof(IImmutableDictionary<string, long>)), Is.True);
            Assert.That(converter.CanConvert(typeof(ImmutableSortedDictionary<int, long>)), Is.True);
            Assert.That(converter.CanConvert(typeof(Dictionary<string, string>)), Is.False);
            Assert.That(converter.CanConvert(typeof(IDictionary<string, string>)), Is.False);
            Assert.That(converter.CanConvert(typeof(string)), Is.False);
            Assert.That(converter.CanWrite, Is.False);
        });
    }

    [Test]
    public void WriteJson_is_not_supported() =>
        Assert.Throws<NotSupportedException>(
            () => new ReadOnlyDictionaryJsonConverter().WriteJson(null!, null, null!));

    /// <summary>A state-shaped holder of every dictionary family the converter reads.</summary>
    public sealed class Holder
    {
        public ReadOnlyDictionary<string, long>? ReadOnly { get; set; }

        public ImmutableDictionary<string, long>? Immutable { get; set; }

        public ImmutableSortedDictionary<int, string>? Sorted { get; set; }

        public IReadOnlyDictionary<string, string>? BehindInterface { get; set; }

        public IReadOnlyDictionary<string, string>? MutableBehindInterface { get; set; }

        public IReadOnlyDictionary<string, string>? ImmutableBehindInterface { get; set; }

        public IReadOnlyDictionary<string, string>? Missing { get; set; }
    }
}
