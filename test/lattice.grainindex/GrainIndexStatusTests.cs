namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexStatus"/>: the assembled report the
/// administrative surface returns, and its null-argument contract.
/// </summary>
[TestFixture]
public sealed class GrainIndexStatusTests
{
    [Test]
    public void A_status_keeps_every_part_it_was_given()
    {
        var definition = DescriptorFactory.Create();
        var fingerprint = new GrainIndexFingerprint("ABCD");
        var drift = new GrainIndexDriftStatus([GrainIndexDefinitionField.Properties]);
        var backfill = GrainIndexBackfillStatus.NotStarted("users");
        var progress = new GrainIndexProgress(1, 2, 50d, "k", null);

        var status = new GrainIndexStatus(
            "users",
            definition,
            registered: true,
            fingerprint,
            "codec-id",
            needsBackfill: true,
            drift,
            backfill,
            progress,
            entryCount: 12);

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo("users"));
            Assert.That(status.Definition, Is.SameAs(definition));
            Assert.That(status.Registered, Is.True);
            Assert.That(status.Fingerprint, Is.EqualTo(fingerprint));
            Assert.That(status.KeyCodecId, Is.EqualTo("codec-id"));
            Assert.That(status.NeedsBackfill, Is.True);
            Assert.That(status.Drift, Is.SameAs(drift));
            Assert.That(status.Backfill, Is.SameAs(backfill));
            Assert.That(status.Progress, Is.SameAs(progress));
            Assert.That(status.EntryCount, Is.EqualTo(12));
        });
    }

    [Test]
    public void An_unregistered_index_still_reports_its_live_declaration()
    {
        var status = Create(registered: false);

        Assert.Multiple(() =>
        {
            Assert.That(status.Registered, Is.False);
            Assert.That(status.Definition, Is.Not.Null);
            Assert.That(status.Drift.HasDrift, Is.False);
        });
    }

    [TestCase("indexName")]
    [TestCase("definition")]
    [TestCase("keyCodecId")]
    [TestCase("drift")]
    [TestCase("backfill")]
    [TestCase("progress")]
    public void A_status_rejects_a_null_required_argument(string argument) =>
        Assert.That(
            () => Create(nullArgument: argument),
            Throws.ArgumentNullException.With.Property("ParamName").EqualTo(argument));

    private static GrainIndexStatus Create(bool registered = true, string? nullArgument = null) =>
        new(
            nullArgument == "indexName" ? null! : "users",
            nullArgument == "definition" ? null! : DescriptorFactory.Create(),
            registered,
            default,
            nullArgument == "keyCodecId" ? null! : "codec-id",
            needsBackfill: false,
            nullArgument == "drift" ? null! : GrainIndexDriftStatus.None,
            nullArgument == "backfill" ? null! : GrainIndexBackfillStatus.NotStarted("users"),
            nullArgument == "progress" ? null! : GrainIndexProgress.None,
            entryCount: 0);
}
