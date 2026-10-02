using Orleans.Lattice.Schema.Tests.Fakes;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4123: <see cref="ILatticeSchemaRemediationGrain.GetStatusAsync"/> is
/// interleaved with a running remediation or migration, so it must answer from the
/// status the coordinator has durably written, never from the live state a phase
/// turn has mutated and is still persisting. Each test holds one state write open on
/// a <see cref="TaskCompletionSource"/> and reads the status inside that window -
/// exactly where an interleaved read lands on a real silo. No timing dependence: the
/// held write suspends the turn synchronously.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private sealed class WriteHold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static WriteHold HoldWrite(FakePersistentState<SchemaRemediationState> state, int attempt)
    {
        var hold = new WriteHold();
        state.BeforeWrite = n =>
        {
            if (n != attempt) return Task.CompletedTask;
            hold.Entered.TrySetResult();
            return hold.Release.Task;
        };
        return hold;
    }

    private static Task ActivateAsync(LatticeSchemaRemediationGrain grain) =>
        ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

    [Test]
    public async Task GetStatusAsync_during_a_held_phase_write_reports_the_durable_phase_not_the_pending_one()
    {
        var h = CreateGrain([("k1", "{\"a\":1}"), ("k2", "{\"a\":2}")], ReservedInFlight(LatticeSchemaRemediationPhase.DryRun));
        await ActivateAsync(h.Grain);
        var hold = HoldWrite(h.State, attempt: 1);

        var pass = h.Grain.RunRemediationPassAsync();
        Assert.That(hold.Entered.Task.IsCompleted, Is.True, "precondition: the DryRun -> Build write is held open");
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build),
            "precondition: the live state already reads Build while its write is pending");

        var during = await h.Grain.GetStatusAsync();

        hold.Release.SetResult();
        await pass;
        var after = await h.Grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during.InProgress, Is.True);
            Assert.That(during.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun),
                "a status read must not report a phase whose write has not yet succeeded");
            Assert.That(during.ScannedCount, Is.Zero);
            Assert.That(during.OperationId, Is.EqualTo("op-rollback"));
            Assert.That(after.Succeeded, Is.True);
            Assert.That(after.ScannedCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task GetStatusAsync_never_reports_a_phase_that_a_failed_write_rolls_back()
    {
        var h = CreateGrain([("k1", "{\"a\":1}")], ReservedInFlight(LatticeSchemaRemediationPhase.DryRun));
        await ActivateAsync(h.Grain);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");
        var hold = HoldWrite(h.State, attempt: 1);

        var pass = h.Grain.RunRemediationPassAsync();
        var during = await h.Grain.GetStatusAsync();
        hold.Release.SetResult();
        Assert.That(async () => await pass, Throws.InvalidOperationException.With.Message.EqualTo("storage down"));
        var after = await h.Grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun),
                "the read inside the window must not report the Build phase the failed write then rolls back");
            Assert.That(after.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun));
            Assert.That(after.InProgress, Is.True);
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun),
                "precondition: the coordinator rolled its live state back");
        });
    }

    [Test]
    public async Task GetStatusAsync_during_a_held_migration_initiate_write_reports_idle_until_the_intent_is_durable()
    {
        var h = CreateGrainBytes([("k1", Env(1, "{\"a\":1}"))], schemaRegistry: MigratingRegistry());
        await ActivateAsync(h.Grain);

        // Write 1 records the alias reservation; write 2 persists the migration intent.
        var hold = HoldWrite(h.State, attempt: 2);

        var start = h.Grain.StartVersionMigrationAsync(MigSchemaId, 2);
        Assert.That(hold.Entered.Task.IsCompleted, Is.True, "precondition: the intent write is held open");
        Assert.That(h.State.State.InProgress, Is.True, "precondition: the live state already reads in-flight");

        var during = await h.Grain.GetStatusAsync();

        hold.Release.SetResult();
        var report = await start;
        var after = await h.Grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during, Is.EqualTo(LatticeSchemaRemediationReport.Idle),
                "no migration is durable yet, so the status read must still report idle");
            Assert.That(report.Succeeded, Is.True);
            Assert.That(after, Is.EqualTo(report));
        });
    }

    [Test]
    public async Task GetStatusAsync_on_an_activated_grain_reports_the_persisted_last_report()
    {
        var persisted = LatticeSchemaRemediationReport.Completed(5, TreeId + "/remediated/op-done", "op-done");
        var h = CreateGrain([], new SchemaRemediationState
        {
            Phase = LatticeSchemaRemediationPhase.Completed,
            LastReport = persisted,
        });
        await ActivateAsync(h.Grain);

        Assert.That(await h.Grain.GetStatusAsync(), Is.EqualTo(persisted));
    }
}
