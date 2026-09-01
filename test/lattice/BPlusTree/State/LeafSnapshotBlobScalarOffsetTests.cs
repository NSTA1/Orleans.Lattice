using Newtonsoft.Json;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit coverage for the two members that carry the nullable scalar-offset
/// representation introduced by issue 1888:
/// <see cref="LeafSnapshotBlob.ScalarOffsetOrSentinel"/>, which folds the three
/// possible stored readings (unset, a legacy negative sentinel, a real covered
/// offset) into the single <c>-1</c>-sentinel scalar the coverage arithmetic works
/// in, and <see cref="LeafSnapshotBlob.NormalizeScalarOffset"/>, which is the
/// inverse used on the write side so a new capture only ever persists
/// <see langword="null"/> or a non-negative offset.
/// </summary>
[TestFixture]
public sealed class LeafSnapshotBlobScalarOffsetTests
{
    [Test]
    public void SnapshotOffset_is_unset_on_a_fresh_blob()
    {
        Assert.That(new LeafSnapshotBlob().SnapshotOffset, Is.Null,
            "The member must equal default(long?) on a fresh instance, so a storage serializer that "
            + "omits type defaults cannot resurrect a value the writer never wrote.");
    }

    [Test]
    public void ScalarOffsetOrSentinel_reports_an_unset_offset_as_the_nothing_captured_sentinel()
    {
        Assert.That(new LeafSnapshotBlob().ScalarOffsetOrSentinel(), Is.EqualTo(-1L));
    }

    /// <summary>
    /// The compatibility leg. A blob persisted before the member became nullable
    /// carries the literal <c>-1</c> - it was non-default for <c>long</c>, so it was
    /// written rather than omitted - and must keep reading as "nothing captured".
    /// </summary>
    [TestCase(-1L)]
    [TestCase(-7L)]
    public void ScalarOffsetOrSentinel_folds_a_legacy_negative_offset_onto_the_same_sentinel(long persisted)
    {
        var blob = new LeafSnapshotBlob { SnapshotOffset = persisted };

        Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(-1L));
    }

    [TestCase(0L)]
    [TestCase(1L)]
    [TestCase(long.MaxValue)]
    public void ScalarOffsetOrSentinel_returns_a_captured_offset_unchanged(long captured)
    {
        var blob = new LeafSnapshotBlob { SnapshotOffset = captured };

        Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(captured),
            "Offset 0 in particular must survive as 0: it is a legitimate captured prefix, and "
            + "collapsing it onto the 'nothing captured' reading is the defect of issue 1888.");
    }

    [TestCase(-1L)]
    [TestCase(-99L)]
    public void NormalizeScalarOffset_stores_a_negative_offset_as_unset(long offset)
    {
        Assert.That(LeafSnapshotBlob.NormalizeScalarOffset(offset), Is.Null,
            "A capture that covers nothing must persist null rather than reintroducing a negative "
            + "sentinel, so there is one canonical representation of 'unset' going forward.");
    }

    [TestCase(0L)]
    [TestCase(42L)]
    public void NormalizeScalarOffset_stores_a_non_negative_offset_verbatim(long offset)
    {
        Assert.That(LeafSnapshotBlob.NormalizeScalarOffset(offset), Is.EqualTo(offset));
    }

    /// <summary>
    /// The pair must round-trip: normalising a scalar and reading it back yields
    /// the same scalar, for every reading including the boundary at zero.
    /// </summary>
    [TestCase(-1L, -1L)]
    [TestCase(-5L, -1L)]
    [TestCase(0L, 0L)]
    [TestCase(9L, 9L)]
    public void NormalizeScalarOffset_and_ScalarOffsetOrSentinel_are_inverses(long input, long expected)
    {
        var blob = new LeafSnapshotBlob { SnapshotOffset = LeafSnapshotBlob.NormalizeScalarOffset(input) };

        Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(expected));
    }

    // --- Backward compatibility with state persisted by an older build ---
    //
    // Grain storage persists these POCOs through Newtonsoft (the same path the
    // committed legacy fixture in LeafSnapshotBlobDualReadTests exercises), so the
    // compatibility question for widening [Id(0)] from long to long? is answered
    // against that serializer rather than in prose. The three shapes an older build
    // could have left behind are a written non-negative offset, the written -1
    // sentinel, and - on a provider that omits type defaults, which is the whole
    // defect - no property at all.

    /// <summary>
    /// A blob persisted by an older build as a plain <c>long</c> deserialises into
    /// the widened nullable member unchanged. This is the case that must not
    /// regress: an existing snapshot has to keep its coverage across the upgrade or
    /// the coverage-gated WAL GC has trimmed a prefix nothing backs.
    /// </summary>
    [TestCase(0L)]
    [TestCase(41L)]
    public void A_legacy_json_blob_carrying_a_captured_offset_deserialises_unchanged(long persisted)
    {
        var legacyJson = $"{{\"SnapshotOffset\":{persisted},\"CapturedAtTicks\":7}}";

        var blob = JsonConvert.DeserializeObject<LeafSnapshotBlob>(legacyJson)!;

        Assert.Multiple(() =>
        {
            Assert.That(blob.SnapshotOffset, Is.EqualTo(persisted));
            Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(persisted));
        });
    }

    /// <summary>
    /// A blob persisted by an older build as the literal <c>-1</c> sentinel keeps
    /// meaning "nothing captured". <c>-1</c> was never a type default, so it was
    /// always written out and will still be present in existing rows.
    /// </summary>
    [Test]
    public void A_legacy_json_blob_carrying_the_minus_one_sentinel_still_reads_as_nothing_captured()
    {
        var blob = JsonConvert.DeserializeObject<LeafSnapshotBlob>("{\"SnapshotOffset\":-1}")!;

        Assert.Multiple(() =>
        {
            Assert.That(blob.SnapshotOffset, Is.EqualTo(-1L),
                "the persisted value is preserved verbatim");
            Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(-1L),
                "and every reader folds it onto the same 'nothing captured' reading as null");
        });
    }

    /// <summary>
    /// The corrupted rows the defect actually produced: a blob whose offset was
    /// written as <c>0</c> and then omitted by a default-dropping provider. Such a
    /// row is indistinguishable from "never captured" no matter what this build
    /// does - the information was destroyed at write time - so it must continue to
    /// read as "nothing captured" rather than becoming a claim of coverage at an
    /// offset the blob cannot substantiate. The repair is prospective: rows written
    /// from now on carry their <c>0</c>.
    /// </summary>
    [Test]
    public void A_legacy_json_blob_with_the_offset_omitted_reads_as_nothing_captured()
    {
        var blob = JsonConvert.DeserializeObject<LeafSnapshotBlob>("{\"CapturedAtTicks\":7}")!;

        Assert.Multiple(() =>
        {
            Assert.That(blob.SnapshotOffset, Is.Null);
            Assert.That(blob.ScalarOffsetOrSentinel(), Is.EqualTo(-1L),
                "an already-corrupted row keeps its pre-existing reading; widening the member cannot "
                + "recover information the old write path destroyed, and must not invent coverage.");
        });
    }

    /// <summary>
    /// The forward direction, proving the repair holds through the real serializer
    /// and not only through the omitting-round-trip harness: an offset of <c>0</c>
    /// is emitted rather than skipped, because <c>default(long?)</c> is
    /// <see langword="null"/>.
    /// </summary>
    [Test]
    public void A_captured_offset_of_zero_is_emitted_by_the_json_serializer_rather_than_treated_as_a_default()
    {
        var json = JsonConvert.SerializeObject(
            new LeafSnapshotBlob { SnapshotOffset = 0L },
            new JsonSerializerSettings { DefaultValueHandling = DefaultValueHandling.Ignore });

        Assert.That(json, Does.Contain("\"SnapshotOffset\":0"),
            "Under DefaultValueHandling.Ignore - the shape of the production provider that dropped the "
            + "value - a nullable member holding 0 is not a default and survives. This is the property "
            + "the whole fix rests on, asserted against the serializer rather than a simulation.");
    }

    /// <summary>
    /// And its converse: an unset offset IS skipped, so "nothing captured" costs no
    /// bytes and reconstructs as <see langword="null"/> rather than as a sentinel.
    /// </summary>
    [Test]
    public void An_unset_offset_is_omitted_by_the_json_serializer_and_reconstructs_as_unset()
    {
        var json = JsonConvert.SerializeObject(
            new LeafSnapshotBlob(),
            new JsonSerializerSettings { DefaultValueHandling = DefaultValueHandling.Ignore });

        Assert.That(json, Does.Not.Contain("SnapshotOffset"));
        Assert.That(JsonConvert.DeserializeObject<LeafSnapshotBlob>(json)!.SnapshotOffset, Is.Null);
    }
}
