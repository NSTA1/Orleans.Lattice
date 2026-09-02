using Newtonsoft.Json;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Backward-compatibility coverage for widening
/// <see cref="AtomicActionState.FailedStepIndex"/> from <c>int</c> to <c>int?</c>
/// (issue 1888). The <c>[Id(4)]</c> slot is unchanged, and grain storage persists
/// this POCO through Newtonsoft, so the question a reviewer asks first - what
/// happens when state persisted by an older build is loaded by the new one - is
/// answered here against that serializer rather than in prose.
/// </summary>
[TestFixture]
public sealed class AtomicActionStateFailedStepIndexCompatibilityTests
{
    [Test]
    public void FailedStepIndex_is_unset_on_a_fresh_instance()
    {
        Assert.That(new AtomicActionState().FailedStepIndex, Is.Null,
            "The member must equal default(int?) on a fresh instance, so a storage serializer that "
            + "omits type defaults cannot resurrect a value the writer never wrote.");
    }

    /// <summary>
    /// A row persisted by an older build as a plain <c>int</c> deserialises into
    /// the widened nullable member unchanged, so a saga already in flight across
    /// the upgrade keeps reporting the step it faulted on.
    /// </summary>
    [TestCase(0)]
    [TestCase(3)]
    public void A_legacy_row_carrying_a_fault_index_deserialises_unchanged(int persisted)
    {
        var legacyJson = $"{{\"Started\":true,\"FailedStepIndex\":{persisted}}}";

        var reloaded = JsonConvert.DeserializeObject<AtomicActionState>(legacyJson)!;

        Assert.That(reloaded.FailedStepIndex, Is.EqualTo(persisted));
    }

    /// <summary>
    /// A row persisted by an older build as the literal <c>-1</c> sentinel keeps
    /// meaning "no forward fault". <c>-1</c> was never a type default, so it was
    /// always written out and will still be present in existing rows; the outcome
    /// projection folds it onto the same reading as <see langword="null"/>.
    /// </summary>
    [Test]
    public void A_legacy_row_carrying_the_minus_one_sentinel_still_reads_as_no_fault()
    {
        var reloaded = JsonConvert.DeserializeObject<AtomicActionState>(
            "{\"Started\":true,\"FailedStepIndex\":-1}")!;

        Assert.That(reloaded.FailedStepIndex, Is.EqualTo(-1),
            "the persisted value is preserved verbatim, and the public outcome projection maps any "
            + "negative index onto the documented -1 'no fault' reading.");
    }

    /// <summary>
    /// The corrupted rows the defect actually produced: a fault on step <c>0</c>
    /// whose index was omitted by a default-dropping provider. The information was
    /// destroyed at write time, so such a row still reads as "no fault" - but it
    /// now does so as <see langword="null"/> rather than by a <c>-1</c> the
    /// initializer invented, and rows written from this build onwards carry their
    /// <c>0</c>.
    /// </summary>
    [Test]
    public void A_legacy_row_with_the_index_omitted_reads_as_unset()
    {
        var reloaded = JsonConvert.DeserializeObject<AtomicActionState>("{\"Started\":true}")!;

        Assert.That(reloaded.FailedStepIndex, Is.Null);
    }

    /// <summary>
    /// The forward direction, proving the repair holds through the real serializer
    /// and not only through the omitting-round-trip harness: a fault on step
    /// <c>0</c> is emitted rather than skipped, because <c>default(int?)</c> is
    /// <see langword="null"/>.
    /// </summary>
    [Test]
    public void A_fault_on_step_zero_is_emitted_by_the_json_serializer_rather_than_treated_as_a_default()
    {
        var json = JsonConvert.SerializeObject(
            new AtomicActionState { Started = true, FailedStepIndex = 0 },
            new JsonSerializerSettings { DefaultValueHandling = DefaultValueHandling.Ignore });

        Assert.That(json, Does.Contain("\"FailedStepIndex\":0"),
            "Under DefaultValueHandling.Ignore - the shape of the production provider that dropped the "
            + "value - a nullable member holding 0 is not a default and survives. That is the whole "
            + "repair, asserted against the serializer rather than a simulation.");
    }

    [Test]
    public void An_unset_index_is_omitted_by_the_json_serializer_and_reconstructs_as_unset()
    {
        var json = JsonConvert.SerializeObject(
            new AtomicActionState { Started = true },
            new JsonSerializerSettings { DefaultValueHandling = DefaultValueHandling.Ignore });

        Assert.That(json, Does.Not.Contain("FailedStepIndex"));
        Assert.That(JsonConvert.DeserializeObject<AtomicActionState>(json)!.FailedStepIndex, Is.Null);
    }
}
