using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4123: the coordinator accepts a remediation without running it, and is
/// then driven one bounded <see cref="LatticeSchemaRemediationGrain.RunSliceAsync"/>
/// at a time, so no single call runs for the whole remediation. Covers the durable
/// cursor each slice resumes after, the phase total the build reports, the banking
/// of a faulted slice's progress (issue #2545), and cancellation before cutover.
/// Every slice is driven synchronously - no timing dependence.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private static readonly (string Key, string Value)[] FiveDocuments =
    [
        ("k1", "{\"a\":1}"), ("k2", "{\"a\":2}"), ("k3", "{\"a\":3}"), ("k4", "{\"a\":4}"), ("k5", "{\"a\":5}"),
    ];

    [Test]
    public async Task AcceptAsync_persists_intent_without_scanning_and_records_the_operation_id()
    {
        var h = CreateGrain(FiveDocuments);

        var accepted = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-accept");

        Assert.Multiple(() =>
        {
            Assert.That(accepted.InProgress, Is.True);
            Assert.That(accepted.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun));
            Assert.That(accepted.OperationId, Is.EqualTo("op-accept"));
            Assert.That(accepted.DestinationTreeId, Does.StartWith(TreeId + "/remediated/").And.Not.Contain("op-accept"),
                "a caller-chosen operation id must never name the destination tree");
            Assert.That(h.State.State.ScanCursor, Is.Null);
        });
        h.Source.DidNotReceiveWithAnyArgs().EntriesAsync();
    }

    [Test]
    public async Task AcceptAsync_with_the_in_flight_parameters_returns_that_operation_and_accepts_nothing_new()
    {
        var h = CreateGrain(FiveDocuments);
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-first");
        var writes = h.State.WriteCount;

        var again = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-second");

        Assert.Multiple(() =>
        {
            Assert.That(again.OperationId, Is.EqualTo("op-first"));
            Assert.That(h.State.WriteCount, Is.EqualTo(writes));
        });
        Assert.That(
            async () => await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), MaxLenPolicy(3), "op-third"),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task RunSliceAsync_processes_one_bounded_slice_at_a_time_through_every_phase()
    {
        var h = CreateGrain(FiveDocuments);
        h.Grain.SliceSize = 2;
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-slices");

        var steps = new List<(LatticeSchemaRemediationPhase Phase, int Scanned, int? Total)>();
        for (var i = 0; i < 20; i++)
        {
            var slice = await h.Grain.RunSliceAsync();
            steps.Add((slice.Report.Phase, slice.Report.ScannedCount, slice.PhaseTotal));
            if (!slice.Report.InProgress)
            {
                break;
            }
        }

        Assert.That(steps, Is.EqualTo(new (LatticeSchemaRemediationPhase, int, int?)[]
        {
            (LatticeSchemaRemediationPhase.DryRun, 2, null),
            (LatticeSchemaRemediationPhase.DryRun, 4, null),
            (LatticeSchemaRemediationPhase.Build, 0, 5),
            (LatticeSchemaRemediationPhase.Build, 2, 5),
            (LatticeSchemaRemediationPhase.Build, 4, 5),
            (LatticeSchemaRemediationPhase.Cutover, 5, null),
            (LatticeSchemaRemediationPhase.Completed, 5, null),
        }));
        foreach (var (key, _) in FiveDocuments)
        {
            await h.Destination.Received(1).SetAsync(key, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        }

        await h.Registry.Received(1).SwapAliasAsync(TreeId, Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task RunSliceAsync_when_nothing_is_in_flight_is_a_no_op()
    {
        var h = CreateGrain(FiveDocuments);

        var slice = await h.Grain.RunSliceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(slice.Report, Is.EqualTo(LatticeSchemaRemediationReport.Idle));
            Assert.That(slice.PhaseTotal, Is.Null);
        });
        h.Source.DidNotReceiveWithAnyArgs().EntriesAsync();
    }

    [Test]
    public async Task A_faulted_build_slice_banks_the_values_it_wrote_and_the_next_slice_resumes_after_them()
    {
        var h = CreateGrain(FiveDocuments);
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-bank");
        await h.Grain.RunSliceAsync();
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build), "precondition");
        var fail = true;
        h.Destination.SetAsync("k3", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ => fail ? Task.FromException(new TimeoutException("destination unavailable")) : Task.CompletedTask);

        Assert.That(async () => await h.Grain.RunSliceAsync(),
            Throws.TypeOf<TimeoutException>().With.Message.EqualTo("destination unavailable"));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ScanCursor, Is.EqualTo("k2"), "the values written before the fault are banked");
            Assert.That(h.State.State.ScannedCount, Is.EqualTo(2));
            Assert.That(h.Grain.GetStatusAsync().Result.ScannedCount, Is.EqualTo(2), "and published");
        });

        fail = false;
        var resumed = await h.Grain.RunSliceAsync();

        Assert.That(resumed.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover));
        await h.Destination.Received(1).SetAsync("k1", Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        await h.Destination.Received(1).SetAsync("k2", Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        await h.Destination.Received(2).SetAsync("k3", Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_faulted_dry_run_slice_banks_its_progress_and_resumes_after_it()
    {
        var failAfterFirst = true;
        var h = CreateGrainCore(
            () => FaultingEntries(() => failAfterFirst),
            ReservedInFlight(LatticeSchemaRemediationPhase.DryRun),
            schemaRegistry: null,
            existingPolicy: null);

        Assert.That(async () => await h.Grain.RunSliceAsync(),
            Throws.TypeOf<InvalidDataException>().With.Message.EqualTo("scan interrupted"));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ScanCursor, Is.EqualTo("k1"));
            Assert.That(h.State.State.ScannedCount, Is.EqualTo(1));
        });

        failAfterFirst = false;
        var resumed = await h.Grain.RunSliceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build));
            Assert.That(resumed.PhaseTotal, Is.EqualTo(3), "the resumed dry run counts the banked value once");
        });
    }

    [Test]
    public void A_bank_that_cannot_be_written_never_masks_the_slice_fault()
    {
        var h = CreateGrainCore(
            () => FaultingEntries(() => true),
            ReservedInFlight(LatticeSchemaRemediationPhase.DryRun),
            schemaRegistry: null,
            existingPolicy: null);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(async () => await h.Grain.RunSliceAsync(),
            Throws.TypeOf<InvalidDataException>().With.Message.EqualTo("scan interrupted"));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.WriteAttempts, Is.EqualTo(1), "precondition: the bank was attempted");
            Assert.That(h.State.State.ScanCursor, Is.Null, "a failed bank rolls its fields back");
            Assert.That(h.State.State.ScannedCount, Is.Zero);
        });
    }

    [Test]
    public async Task CancelAsync_during_the_build_discards_the_destination_and_records_cancelled()
    {
        var seed = ReservedInFlight(LatticeSchemaRemediationPhase.Build);
        seed.ScannedCount = 2;
        var h = CreateGrain(FiveDocuments, seed);

        var report = await h.Grain.CancelAsync("op-rollback");

        Assert.Multiple(() =>
        {
            Assert.That(report.WasCancelled, Is.True);
            Assert.That(report.InProgress, Is.False);
            Assert.That(report.ScannedCount, Is.EqualTo(2));
            Assert.That(report.OperationId, Is.EqualTo("op-rollback"));
            Assert.That(h.State.State.LastReport, Is.EqualTo(report));
        });
        await h.Destination.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        await h.Registry.DidNotReceiveWithAnyArgs().SwapAliasAsync(default!, default!, default!, default, default);
        await h.PolicyStore.DidNotReceiveWithAnyArgs().SetPolicyAsync(default!, default!, default);
    }

    [Test]
    public async Task CancelAsync_during_the_dry_run_records_cancelled_without_touching_a_destination()
    {
        var h = CreateGrain(FiveDocuments, ReservedInFlight(LatticeSchemaRemediationPhase.DryRun));

        var report = await h.Grain.CancelAsync("op-rollback");

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cancelled));
        await h.Destination.DidNotReceiveWithAnyArgs().DeleteTreeAsync(default);
    }

    [Test]
    public async Task CancelAsync_at_cutover_is_declined_and_the_remediation_completes()
    {
        var h = CreateGrain(FiveDocuments, ReservedInFlight(LatticeSchemaRemediationPhase.Cutover));

        var declined = await h.Grain.CancelAsync("op-rollback");
        var finished = await h.Grain.RunSliceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(declined.InProgress, Is.True);
            Assert.That(declined.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover));
            Assert.That(finished.Report.Succeeded, Is.True);
        });
    }

    [Test]
    public async Task CancelAsync_for_another_operation_leaves_the_remediation_running()
    {
        var h = CreateGrain(FiveDocuments, ReservedInFlight(LatticeSchemaRemediationPhase.Build));

        var report = await h.Grain.CancelAsync("op-other");

        Assert.Multiple(() =>
        {
            Assert.That(report.InProgress, Is.True);
            Assert.That(h.State.WriteCount, Is.Zero);
        });
        await h.Destination.DidNotReceiveWithAnyArgs().DeleteTreeAsync(default);
    }

    [Test]
    public void CancelAsync_and_AcceptAsync_reject_an_empty_operation_id()
    {
        var h = CreateGrain(FiveDocuments);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Grain.CancelAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), ""),
                Throws.ArgumentException);
            Assert.That(async () => await h.Grain.AcceptVersionMigrationAsync(MigSchemaId, 2, ""),
                Throws.ArgumentException);
        });
    }

    [Test]
    public async Task AcceptVersionMigrationAsync_on_a_tree_already_migrated_returns_its_completed_report()
    {
        var h = CreateGrainBytes([("k1", Env(1, "{\"a\":1}"))], schemaRegistry: MigratingRegistry());
        var first = await h.Grain.StartVersionMigrationAsync(MigSchemaId, 2);
        var writes = h.State.WriteCount;

        var again = await h.Grain.AcceptVersionMigrationAsync(MigSchemaId, 2, "op-again");

        Assert.Multiple(() =>
        {
            Assert.That(again, Is.EqualTo(first));
            Assert.That(again.Succeeded, Is.True);
            Assert.That(h.State.WriteCount, Is.EqualTo(writes));
        });
    }

    /// <summary>
    /// Yields <c>k1</c>, then faults while <paramref name="fail"/> holds, otherwise
    /// yields <c>k2</c> and <c>k3</c>; honours a resume bound through the harness.
    /// </summary>
    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> FaultingEntries(Func<bool> fail)
    {
        yield return new KeyValuePair<string, byte[]>("k1", Utf8("{\"a\":1}"));
        await Task.CompletedTask;
        if (fail())
        {
            throw new InvalidDataException("scan interrupted");
        }

        yield return new KeyValuePair<string, byte[]>("k2", Utf8("{\"a\":2}"));
        yield return new KeyValuePair<string, byte[]>("k3", Utf8("{\"a\":3}"));
    }
}
