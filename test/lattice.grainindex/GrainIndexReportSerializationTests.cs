using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The administrative report surface - <see cref="GrainIndexStatus"/> and the
/// <see cref="GrainIndexProgress"/>, <see cref="GrainIndexDriftStatus"/> and
/// <see cref="GrainIndexBackfillStatus"/> reports it nests - together with the
/// <see cref="GrainIndexMatch"/> a query returns and the
/// <see cref="GrainIndexBackfillBatchResult"/> a backfill pass returns, all
/// cross a silo or client boundary the moment an operator reads a report or a
/// caller runs a query. Their Orleans wire format is therefore part of this
/// package's contract, and these tests round-trip each one through the real
/// serializer.
/// <para>
/// Every value under test is built fully populated - no member left at its
/// default - and every member is asserted <i>individually</i> rather than by
/// comparing whole values. That is the point of the fixture: a record's
/// structural equality still holds when both sides lose the same member, so a
/// dropped, duplicated or renumbered <c>[Id]</c> would slip straight through a
/// whole-value comparison. One assertion per member pins one <c>[Id]</c>.
/// </para>
/// <para>
/// Distinct values are chosen per member for the same reason: two numeric
/// members that both read <c>0</c>, or two flags that are both <c>true</c>,
/// cannot detect having been swapped. Where a type has two booleans, a separate
/// test drives them apart.
/// </para>
/// </summary>
[TestFixture]
public sealed class GrainIndexReportSerializationTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value) => _serializer.Deserialize<T>(_serializer.SerializeToArray(value));

    /// <summary>Fixed instants, so the fixture never depends on the clock.</summary>
    private static readonly DateTimeOffset Started = new(2026, 8, 31, 9, 45, 57, TimeSpan.FromHours(1));

    private static readonly DateTimeOffset Updated = new(2026, 8, 31, 10, 15, 3, TimeSpan.FromHours(2));

    private static readonly DateTimeOffset Completed = new(2026, 8, 31, 11, 2, 11, TimeSpan.Zero);

    private static GrainIndexProgress PopulatedProgress() =>
        new(processed: 1234, total: 9876, percentComplete: 12.5d, lastProcessedKey: "grain-1234", lastError: "pass 7 threw");

    private static GrainIndexDriftStatus PopulatedDrift() =>
        new([
            GrainIndexDefinitionField.Name,
            GrainIndexDefinitionField.TreeName,
            GrainIndexDefinitionField.GrainInterfaceType,
            GrainIndexDefinitionField.StateType,
            GrainIndexDefinitionField.KeyCodec,
            GrainIndexDefinitionField.Properties,
            GrainIndexDefinitionField.AllowReplication,
        ]);

    private static GrainIndexBackfillStatus PopulatedBackfill() =>
        new(
            "users",
            GrainIndexBackfillState.Paused,
            resumeAfterKey: "grain-0042",
            visited: 91,
            enrolled: 55,
            skipped: 30,
            failed: 6,
            passes: 7,
            revisitsEnrolled: true,
            startedUtc: Started,
            updatedUtc: Updated,
            completedUtc: Completed,
            failureMessage: "the key source threw on pass 7");

    private static GrainIndexStatus PopulatedStatus(bool registered = true, bool needsBackfill = true) =>
        new(
            "users",
            DescriptorFactory.Create(allowReplication: true),
            registered,
            new GrainIndexFingerprint("0123456789ABCDEF0123456789ABCDEF"),
            "codec-id",
            needsBackfill,
            PopulatedDrift(),
            PopulatedBackfill(),
            PopulatedProgress(),
            entryCount: 4321);

    [Test]
    public void A_progress_report_round_trips_every_member()
    {
        var progress = PopulatedProgress();

        var copy = RoundTrip(progress);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.SameAs(progress), "The round trip must produce a new instance.");
            Assert.That(copy.Processed, Is.EqualTo(1234L));
            Assert.That(copy.Total, Is.EqualTo(9876L));
            Assert.That(copy.PercentComplete, Is.EqualTo(12.5d));
            Assert.That(copy.LastProcessedKey, Is.EqualTo("grain-1234"));
            Assert.That(copy.LastError, Is.EqualTo("pass 7 threw"));
        });
    }

    [Test]
    public void A_progress_report_round_trips_an_unbounded_population_as_null()
    {
        // A key source that cannot bound its population reports null rather than
        // a fabricated denominator, so null must survive the wire as null and not
        // arrive as zero.
        var copy = RoundTrip(new GrainIndexProgress(processed: 12, total: null, percentComplete: null, lastProcessedKey: null, lastError: null));

        Assert.Multiple(() =>
        {
            Assert.That(copy.Processed, Is.EqualTo(12L));
            Assert.That(copy.Total, Is.Null);
            Assert.That(copy.PercentComplete, Is.Null);
            Assert.That(copy.LastProcessedKey, Is.Null);
            Assert.That(copy.LastError, Is.Null);
        });
    }

    [Test]
    public void A_drift_status_round_trips_every_changed_field_in_declaration_order()
    {
        var drift = PopulatedDrift();

        var copy = RoundTrip(drift);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.SameAs(drift), "The round trip must produce a new instance.");
            Assert.That(copy.ChangedFields, Is.EqualTo(drift.ChangedFields).AsCollection);
            Assert.That(copy.HasDrift, Is.True);
            Assert.That(copy.HasBreakingChange, Is.True);
        });
    }

    [Test]
    public void An_empty_drift_status_round_trips_as_no_drift()
    {
        var copy = RoundTrip(GrainIndexDriftStatus.None);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ChangedFields, Is.Empty);
            Assert.That(copy.HasDrift, Is.False);
            Assert.That(copy.HasBreakingChange, Is.False);
        });
    }

    [Test]
    public void A_backfill_status_round_trips_every_member()
    {
        var backfill = PopulatedBackfill();

        var copy = RoundTrip(backfill);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.SameAs(backfill), "The round trip must produce a new instance.");
            Assert.That(copy.IndexName, Is.EqualTo("users"));
            Assert.That(copy.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(copy.ResumeAfterKey, Is.EqualTo("grain-0042"));
            Assert.That(copy.Visited, Is.EqualTo(91L));
            Assert.That(copy.Enrolled, Is.EqualTo(55L));
            Assert.That(copy.Skipped, Is.EqualTo(30L));
            Assert.That(copy.Failed, Is.EqualTo(6L));
            Assert.That(copy.Passes, Is.EqualTo(7L));
            Assert.That(copy.RevisitsEnrolled, Is.True);
            Assert.That(copy.StartedUtc, Is.EqualTo(Started));
            Assert.That(copy.UpdatedUtc, Is.EqualTo(Updated));
            Assert.That(copy.CompletedUtc, Is.EqualTo(Completed));
            Assert.That(copy.FailureMessage, Is.EqualTo("the key source threw on pass 7"));
        });
    }

    [Test]
    public void A_backfill_status_round_trips_its_three_timestamps_by_offset()
    {
        // The three instants carry three different UTC offsets on purpose: a
        // codec that normalised them to UTC, or that transposed two of the
        // three [Id]s, would be invisible if they all shared an offset.
        var copy = RoundTrip(PopulatedBackfill());

        Assert.Multiple(() =>
        {
            Assert.That(copy.StartedUtc!.Value.Offset, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(copy.UpdatedUtc!.Value.Offset, Is.EqualTo(TimeSpan.FromHours(2)));
            Assert.That(copy.CompletedUtc!.Value.Offset, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void A_never_started_backfill_status_round_trips_its_absent_members_as_null()
    {
        var copy = RoundTrip(GrainIndexBackfillStatus.NotStarted("users"));

        Assert.Multiple(() =>
        {
            Assert.That(copy.IndexName, Is.EqualTo("users"));
            Assert.That(copy.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(copy.ResumeAfterKey, Is.Null);
            Assert.That(copy.StartedUtc, Is.Null);
            Assert.That(copy.UpdatedUtc, Is.Null);
            Assert.That(copy.CompletedUtc, Is.Null);
            Assert.That(copy.FailureMessage, Is.Null);
        });
    }

    [Test]
    public void A_backfill_batch_result_round_trips_every_member()
    {
        var result = new GrainIndexBackfillBatchResult(
            visited: 41,
            enrolled: 23,
            skipped: 13,
            failed: 5,
            GrainIndexBackfillState.Failed,
            exhausted: true);

        var copy = RoundTrip(result);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Visited, Is.EqualTo(41));
            Assert.That(copy.Enrolled, Is.EqualTo(23));
            Assert.That(copy.Skipped, Is.EqualTo(13));
            Assert.That(copy.Failed, Is.EqualTo(5));
            Assert.That(copy.State, Is.EqualTo(GrainIndexBackfillState.Failed));
            Assert.That(copy.Exhausted, Is.True);
        });
    }

    [Test]
    public void A_match_round_trips_every_member()
    {
        var payload = Encoding.UTF8.GetBytes("{\"Age\":18,\"__k\":\"alice\"}");
        var match = new GrainIndexMatch("alice", "Age", payload);

        var copy = RoundTrip(match);

        Assert.Multiple(() =>
        {
            Assert.That(copy.GrainKey, Is.EqualTo("alice"));
            Assert.That(copy.PropertyName, Is.EqualTo("Age"));
            Assert.That(copy.Value, Is.EqualTo(payload).AsCollection);
            Assert.That(copy.Value, Is.Not.SameAs(payload), "The payload must arrive as its own buffer.");
        });
    }

    [Test]
    public void A_match_round_trips_an_identity_only_payload_as_empty()
    {
        // A query that asked only for grain identities never transfers a payload,
        // so the empty buffer has to survive as empty rather than as null.
        var copy = RoundTrip(new GrainIndexMatch("alice", "Age", []));

        Assert.Multiple(() =>
        {
            Assert.That(copy.GrainKey, Is.EqualTo("alice"));
            Assert.That(copy.PropertyName, Is.EqualTo("Age"));
            Assert.That(copy.Value, Is.Not.Null);
            Assert.That(copy.Value, Is.Empty);
        });
    }

    [Test]
    public void A_status_report_round_trips_every_top_level_member()
    {
        var status = PopulatedStatus();

        var copy = RoundTrip(status);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.SameAs(status), "The round trip must produce a new instance.");
            Assert.That(copy.IndexName, Is.EqualTo("users"));
            Assert.That(copy.Definition, Is.Not.Null);
            Assert.That(copy.Registered, Is.True);
            Assert.That(copy.Fingerprint, Is.EqualTo(new GrainIndexFingerprint("0123456789ABCDEF0123456789ABCDEF")));
            Assert.That(copy.KeyCodecId, Is.EqualTo("codec-id"));
            Assert.That(copy.NeedsBackfill, Is.True);
            Assert.That(copy.Drift, Is.Not.Null);
            Assert.That(copy.Backfill, Is.Not.Null);
            Assert.That(copy.Progress, Is.Not.Null);
            Assert.That(copy.EntryCount, Is.EqualTo(4321L));
        });
    }

    [Test]
    public void A_status_report_keeps_its_two_boolean_flags_apart()
    {
        // Registered is [Id(2)] and NeedsBackfill is [Id(5)]. Both true in the
        // fully-populated case cannot detect a transposition, so drive them apart.
        var copy = RoundTrip(PopulatedStatus(registered: true, needsBackfill: false));
        var flipped = RoundTrip(PopulatedStatus(registered: false, needsBackfill: true));

        Assert.Multiple(() =>
        {
            Assert.That(copy.Registered, Is.True);
            Assert.That(copy.NeedsBackfill, Is.False);
            Assert.That(flipped.Registered, Is.False);
            Assert.That(flipped.NeedsBackfill, Is.True);
        });
    }

    [Test]
    public void A_status_report_round_trips_its_nested_definition_member_by_member()
    {
        var definition = DescriptorFactory.Create(allowReplication: true);

        var copy = RoundTrip(PopulatedStatus()).Definition;

        Assert.Multiple(() =>
        {
            Assert.That(copy.Name, Is.EqualTo(definition.Name));
            Assert.That(copy.TreeName, Is.EqualTo(definition.TreeName));
            Assert.That(copy.GrainInterfaceTypeName, Is.EqualTo(definition.GrainInterfaceTypeName));
            Assert.That(copy.StateTypeName, Is.EqualTo(definition.StateTypeName));
            Assert.That(copy.Properties.Select(p => p.Name), Is.EqualTo(definition.Properties.Select(p => p.Name)).AsCollection);
            Assert.That(
                copy.Properties.Select(p => p.PropertyTypeName),
                Is.EqualTo(definition.Properties.Select(p => p.PropertyTypeName)).AsCollection);
            Assert.That(copy.AllowReplication, Is.True);
        });
    }

    [Test]
    public void A_status_report_round_trips_its_nested_drift_progress_and_backfill_reports()
    {
        var copy = RoundTrip(PopulatedStatus());

        Assert.Multiple(() =>
        {
            Assert.That(copy.Drift.ChangedFields, Is.EqualTo(PopulatedDrift().ChangedFields).AsCollection);
            Assert.That(copy.Drift.HasBreakingChange, Is.True);

            Assert.That(copy.Progress.Processed, Is.EqualTo(1234L));
            Assert.That(copy.Progress.Total, Is.EqualTo(9876L));
            Assert.That(copy.Progress.PercentComplete, Is.EqualTo(12.5d));
            Assert.That(copy.Progress.LastProcessedKey, Is.EqualTo("grain-1234"));
            Assert.That(copy.Progress.LastError, Is.EqualTo("pass 7 threw"));

            Assert.That(copy.Backfill.IndexName, Is.EqualTo("users"));
            Assert.That(copy.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(copy.Backfill.ResumeAfterKey, Is.EqualTo("grain-0042"));
            Assert.That(copy.Backfill.Visited, Is.EqualTo(91L));
            Assert.That(copy.Backfill.Enrolled, Is.EqualTo(55L));
            Assert.That(copy.Backfill.Skipped, Is.EqualTo(30L));
            Assert.That(copy.Backfill.Failed, Is.EqualTo(6L));
            Assert.That(copy.Backfill.Passes, Is.EqualTo(7L));
            Assert.That(copy.Backfill.RevisitsEnrolled, Is.True);
            Assert.That(copy.Backfill.StartedUtc, Is.EqualTo(Started));
            Assert.That(copy.Backfill.UpdatedUtc, Is.EqualTo(Updated));
            Assert.That(copy.Backfill.CompletedUtc, Is.EqualTo(Completed));
            Assert.That(copy.Backfill.FailureMessage, Is.EqualTo("the key source threw on pass 7"));
        });
    }
}
