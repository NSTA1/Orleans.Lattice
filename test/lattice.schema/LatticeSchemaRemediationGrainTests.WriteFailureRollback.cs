using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Schema.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Coverage for every <c>WriteStateAsync</c>-failure rollback arm in
/// <see cref="LatticeSchemaRemediationGrain"/>: the coordinator mutates its state
/// in memory, persists, and restores every field it touched when the persist
/// throws - so a storage fault leaves the grain reporting exactly what is durable
/// rather than a phase that was never written.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these need their own fixture, and why the alias reservation is seeded.</b>
/// Every remediation pass opens with <c>ReserveAliasAsync</c>, which itself writes
/// state when no reservation is held. The shared fake's <c>ThrowOnWrite</c> is a
/// one-shot - it clears itself after firing - so a test that merely sets it and
/// runs a pass faults inside the reservation write and never reaches the phase,
/// complete, or abort arm it is named for. Those tests still pass, because the
/// assertion they make (the phase did not advance) is equally true when the pass
/// aborted one step earlier: the fixture is green and the arm is cold.
/// </para>
/// <para>
/// Seeding <c>AliasReservationId</c> removes that first write entirely - the
/// reservation branch is skipped when one is already held - so the one-shot fault
/// lands on the write actually under test. That is also a faithful scenario rather
/// than a trick: a resumed pass after a reactivation holds its reservation
/// already, which is precisely when a storage fault mid-remediation is most
/// consequential.
/// </para>
/// <para>
/// Each rollback is asserted field by field over a seed whose fields all differ
/// from the values the failed write would have left behind, so an arm that
/// restored only some of them fails here. Each is also paired with the same
/// operation succeeding over the same setup, which is what proves the fixture
/// reaches the write at all rather than faulting before it.
/// </para>
/// </remarks>
public partial class LatticeSchemaRemediationGrainTests
{
    private const string HeldReservation = "remediation:held";

    /// <summary>
    /// An in-flight transform whose alias reservation is already held, so the pass
    /// performs no reservation write and the seeded fault lands on the arm under
    /// test.
    /// </summary>
    private static SchemaRemediationState ReservedInFlight(
        LatticeSchemaRemediationPhase phase,
        LatticeSchemaPolicy? policy = null) =>
        new()
        {
            InProgress = true,
            Phase = phase,
            OperationId = "op-rollback",
            DestinationTreeId = TreeId + "/remediated/op-rollback",
            SourcePhysicalTreeId = TreeId,
            Transform = LatticeValueTransform.Passthrough(),
            TargetPolicy = policy ?? JsonPolicy(),
            AliasReservationId = HeldReservation,
        };

    // ----- AdvancePhaseAsync -----

    [Test]
    public async Task AdvancePhase_write_failure_restores_the_phase_and_scanned_count()
    {
        var seed = ReservedInFlight(LatticeSchemaRemediationPhase.DryRun);
        seed.ScannedCount = 3;
        var h = CreateGrain([("k1", "{\"a\":1}")], seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("storage down"));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun),
                "an unpersisted phase advance must not be visible in memory");
            Assert.That(h.State.State.ScannedCount, Is.EqualTo(3),
                "the scanned count advances with the phase and must roll back with it");
            Assert.That(h.State.WriteCount, Is.Zero,
                "nothing was persisted, so the rollback is the only record of the attempt");
        });

        // The same pass over the same setup, without the fault, must actually reach
        // the advance - otherwise the assertions above hold for a pass that never
        // got there.
        var ok = CreateGrain([("k1", "{\"a\":1}")], ReservedInFlight(LatticeSchemaRemediationPhase.DryRun));
        await ok.Grain.RunRemediationPassAsync();
        Assert.That(ok.State.State.Phase, Is.Not.EqualTo(LatticeSchemaRemediationPhase.DryRun),
            "positive control: the unfaulted pass must leave the dry-run phase");
    }

    // ----- AbortAsync -----

    [Test]
    public async Task Abort_write_failure_restores_progress_phase_report_and_count()
    {
        var seed = ReservedInFlight(LatticeSchemaRemediationPhase.DryRun, MaxLenPolicy(3));
        seed.ScannedCount = 7;
        var h = CreateGrain([("k1", "{\"too\":\"big\"}")], seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("storage down"));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.True,
                "an unpersisted abort must leave the remediation in flight, so a resume retries it");
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun));
            Assert.That(h.State.State.LastReport, Is.Null,
                "reporting an abort that was never persisted would claim a durable outcome that does not exist");
            Assert.That(h.State.State.ScannedCount, Is.EqualTo(7));
        });

        var ok = CreateGrain([("k1", "{\"too\":\"big\"}")], ReservedInFlight(LatticeSchemaRemediationPhase.DryRun, MaxLenPolicy(3)));
        await ok.Grain.RunRemediationPassAsync();
        Assert.Multiple(() =>
        {
            Assert.That(ok.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted),
                "positive control: the unfaulted pass must actually abort");
            Assert.That(ok.State.State.LastReport!.Value.OffendingKey, Is.EqualTo("k1"));
        });
    }

    // ----- CompleteAsync -----

    [Test]
    public async Task Complete_write_failure_restores_progress_phase_and_report()
    {
        var seed = ReservedInFlight(LatticeSchemaRemediationPhase.Cutover);
        seed.ScannedCount = 5;
        var h = CreateGrain([], seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("storage down"));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.True);
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover),
                "the cutover already happened; only its durable completion failed, so a resume must re-complete");
            Assert.That(h.State.State.LastReport, Is.Null);
        });

        var ok = CreateGrain([], ReservedInFlight(LatticeSchemaRemediationPhase.Cutover));
        await ok.Grain.RunRemediationPassAsync();
        Assert.That(ok.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed),
            "positive control: the unfaulted pass must actually complete");
    }

    [Test]
    public void Complete_write_failure_restores_the_last_completed_migration_version()
    {
        // The migration branch of CompleteAsync stamps an extra field, and it is the
        // one a repeat MigrateToTargetVersionAsync short-circuits on. Restoring the
        // rest but leaving this stamped would make a failed migration report itself
        // as already done and skip the work on every retry.
        var seed = ReservedInFlight(LatticeSchemaRemediationPhase.Cutover);
        seed.Mode = SchemaRemediationMode.SchemaVersionMigration;
        seed.MigrationSchemaId = MigSchemaId;
        seed.MigrationTargetVersion = 4;
        seed.LastCompletedMigrationVersion = 2;
        var h = CreateGrain([], seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException);

        Assert.That(h.State.State.LastCompletedMigrationVersion, Is.EqualTo(2u),
            "an unpersisted migration must not advance the version a retry short-circuits on");
    }

    // ----- InitiateCoreAsync -----

    [Test]
    public async Task Initiate_write_failure_restores_every_field_it_staged()
    {
        // The widest rollback in the coordinator: twelve fields staged before the
        // first persist. A previous remediation's durable record is what it restores
        // to, so the seed carries a completed one rather than a blank state - a
        // rollback to `default` would look correct against an empty seed.
        var previousPolicy = MaxLenPolicy(512);
        var previous = new SchemaRemediationState
        {
            InProgress = false,
            Phase = LatticeSchemaRemediationPhase.Completed,
            OperationId = "op-previous",
            DestinationTreeId = TreeId + "/remediated/op-previous",
            Transform = LatticeValueTransform.DropMember("legacy"),
            TargetPolicy = previousPolicy,
            LastReport = LatticeSchemaRemediationReport.Completed(11, TreeId + "/remediated/op-previous", "op-previous"),
            ScannedCount = 11,
            SourcePhysicalTreeId = TreeId + "/remediated/op-previous",
            Mode = SchemaRemediationMode.SchemaVersionMigration,
            MigrationSchemaId = MigSchemaId,
            MigrationTargetVersion = 3,
        };
        var h = CreateGrain([("k1", "{\"a\":1}")], previous);

        // InitiateCoreAsync reserves the alias (one write) before it persists the
        // remediation intent (the second), so the fault is pinned to the second
        // write. A one-shot fault on the first would abort inside the reservation
        // and never reach the rollback this test is named for.
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");
        h.State.ThrowOnWriteNumber = 2;

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.InvalidOperationException.With.Message.EqualTo("storage down"));

        var s = h.State.State;
        Assert.Multiple(() =>
        {
            Assert.That(h.State.WriteAttempts, Is.EqualTo(2),
                "the fault must land on the intent write, not on the alias reservation before it");
            Assert.That(s.InProgress, Is.False, "a remediation whose intent never persisted must not read as in flight");
            Assert.That(s.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed));
            Assert.That(s.OperationId, Is.EqualTo("op-previous"));
            Assert.That(s.DestinationTreeId, Is.EqualTo(TreeId + "/remediated/op-previous"));
            Assert.That(s.Transform.Kind, Is.EqualTo(LatticeValueTransform.DropMember("legacy").Kind));
            Assert.That(s.TargetPolicy, Is.SameAs(previousPolicy),
                "the previously consented policy instance must be restored, not a rebuilt equivalent");
            Assert.That(s.LastReport, Is.Not.Null, "the previous durable outcome must survive a failed start");
            Assert.That(s.LastReport!.Value.OperationId, Is.EqualTo("op-previous"));
            Assert.That(s.ScannedCount, Is.EqualTo(11));
            Assert.That(s.SourcePhysicalTreeId, Is.EqualTo(TreeId + "/remediated/op-previous"));
            Assert.That(s.Mode, Is.EqualTo(SchemaRemediationMode.SchemaVersionMigration));
            Assert.That(s.MigrationSchemaId, Is.EqualTo(MigSchemaId));
            Assert.That(s.MigrationTargetVersion, Is.EqualTo(3u));
        });

        // Positive control over the same seed: without the fault, the start does
        // overwrite every one of those fields, so the assertions above are about a
        // rollback rather than about a start that never ran.
        var ok = CreateGrain([("k1", "{\"a\":1}")], new SchemaRemediationState
        {
            Phase = LatticeSchemaRemediationPhase.Completed,
            OperationId = "op-previous",
            Mode = SchemaRemediationMode.SchemaVersionMigration,
        });
        await ok.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy());
        Assert.Multiple(() =>
        {
            Assert.That(ok.State.State.OperationId, Is.Not.EqualTo("op-previous"));
            Assert.That(ok.State.State.Mode, Is.EqualTo(SchemaRemediationMode.Transform));
        });
    }

    // ----- ReleaseAliasAsync -----

    [Test]
    public async Task ReleaseAlias_write_failure_restores_the_reservation_id()
    {
        // The reservation is what blocks a concurrent alias change. Clearing it in
        // memory after a failed persist would let the next pass believe no
        // reservation is held while the durable record still names one, stranding it.
        var h = CreateGrain([], new SchemaRemediationState { AliasReservationId = HeldReservation });
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("storage down"));

        Assert.That(h.State.State.AliasReservationId, Is.EqualTo(HeldReservation),
            "an unpersisted release must leave the reservation held in memory too");

        var ok = CreateGrain([], new SchemaRemediationState { AliasReservationId = HeldReservation });
        await ok.Grain.RunRemediationPassAsync();
        Assert.That(ok.State.State.AliasReservationId, Is.Null,
            "positive control: the unfaulted idle pass must actually release");
    }
}
