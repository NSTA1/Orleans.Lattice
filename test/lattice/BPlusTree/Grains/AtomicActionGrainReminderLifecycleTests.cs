using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Reminder-driven lifecycle, plan-validation and keepalive-containment coverage
/// for <see cref="AtomicActionGrain"/>.
/// </summary>
/// <remarks>
/// <para>
/// The saga multiplexes two reminders through <c>IRemindable</c>: a retention
/// reminder that clears the terminal record once its window elapses, and a
/// keepalive reminder that resumes a saga whose activation died mid-flight. Both
/// are the mechanism by which an in-flight saga survives a silo crash, so their
/// arms are load-bearing even though nothing calls them synchronously.
/// </para>
/// <para>
/// The keepalive branch is also the one place a resumed saga's failure is
/// deliberately swallowed: a reminder tick must never fault back into the Orleans
/// reminder service, or the tick is retried forever. The tests below pin that
/// containment, the terminal-phase teardown, and the plan guards that reject a
/// malformed step before any effect runs.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AtomicActionGrainReminderLifecycleTests
{
    private const string OperationId = "op-lifecycle";
    private const string KeepaliveReminder = "atomic-action-keepalive";
    private const string RetentionReminder = "atomic-action-retention";

    private sealed record Harness(
        AtomicActionGrain Grain,
        FakePersistentState<AtomicActionState> State,
        IReminderRegistry Reminders,
        List<string> Trace);

    private static Harness CreateGrain(
        FakePersistentState<AtomicActionState>? existingState = null,
        LatticeOptions? options = null,
        IReminderRegistry? reminderRegistry = null,
        Func<IAtomicActionContext, Task>? forward = null,
        Func<IAtomicActionContext, Task>? compensate = null)
    {
        var trace = new List<string>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-action", OperationId));

        var catalog = new AtomicActionCatalog(new Dictionary<string, AtomicActionHandlerRegistration>(StringComparer.Ordinal)
        {
            ["h"] = new(
                new DelegateAtomicActionHandler(
                    "h",
                    "v1",
                    forward ?? (ctx => { trace.Add("fwd"); return Task.CompletedTask; }),
                    compensate ?? (ctx => { trace.Add("comp"); return Task.CompletedTask; })),
                "v1"),
        });

        var reminders = reminderRegistry ?? Substitute.For<IReminderRegistry>();
        var opts = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<AtomicActionState>();

        var grain = new AtomicActionGrain(
            context,
            Substitute.For<IGrainFactory>(),
            reminders,
            optionsMonitor,
            catalog,
            new LoggerFactory().CreateLogger<AtomicActionGrain>(),
            state);

        return new Harness(grain, state, reminders, trace);
    }

    private static AtomicActionStep CustomStep(string handlerId = "h") => new()
    {
        Kind = AtomicActionStepKind.Custom,
        HandlerId = handlerId,
        ArgsPayload = [],
    };

    /// <summary>
    /// A step as it exists on a persisted, already-started saga: the version tag
    /// has been stamped from the registered handler, which
    /// <c>ResolveHandlerForResume</c> re-checks on every resume so a handler that
    /// changed under an in-flight saga parks it instead of replaying a changed
    /// effect. A resume fixture that seeds an unstamped step exercises that park,
    /// not the resume, so the tag is stamped here deliberately.
    /// </summary>
    private static AtomicActionStep StampedStep(string handlerId = "h") => new()
    {
        Kind = AtomicActionStepKind.Custom,
        HandlerId = handlerId,
        ArgsPayload = [],
        VersionTag = "v1",
    };

    private static FakePersistentState<AtomicActionState> ResumableState(
        AtomicActionPhase phase,
        bool started = true)
    {
        var state = new FakePersistentState<AtomicActionState>();
        state.State.Phase = phase;
        state.State.Started = started;
        state.State.Steps = [StampedStep()];
        state.State.StepStatuses = [AtomicActionStepStatus.Pending];
        return state;
    }

    // ----- Retention reminder (TTL expiry) -----

    [Test]
    public async Task Retention_reminder_clears_the_terminal_record()
    {
        var state = new FakePersistentState<AtomicActionState>();
        state.State.Phase = AtomicActionPhase.Committed;
        state.State.Started = true;
        state.State.Steps = [StampedStep()];
        state.State.StepStatuses = [AtomicActionStepStatus.ForwardDone];

        var reminder = Substitute.For<IGrainReminder>();
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), RetentionReminder)
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        var h = CreateGrain(existingState: state, reminderRegistry: registry);

        await h.Grain.ReceiveReminder(RetentionReminder, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Forward),
                "ClearStateAsync resets the record to its default phase.");
            Assert.That(h.State.State.Started, Is.False);
            Assert.That(h.State.State.Steps, Is.Empty);
        });
        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task Retention_reminder_still_unregisters_when_the_clear_fails()
    {
        // TtlGrain contains a failing cleanup so the reminder is not retried
        // forever against a grain whose storage is unavailable.
        var state = new FakePersistentState<AtomicActionState>
        {
            ThrowOnClear = new InvalidOperationException("storage down"),
        };
        state.State.Phase = AtomicActionPhase.Committed;

        var reminder = Substitute.For<IGrainReminder>();
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), RetentionReminder)
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        var h = CreateGrain(existingState: state, reminderRegistry: registry);

        Assert.DoesNotThrowAsync(() => h.Grain.ReceiveReminder(RetentionReminder, new TickStatus()));

        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    // ----- Keepalive reminder (crash resume) -----

    [Test]
    public async Task Keepalive_reminder_resumes_a_started_forward_saga()
    {
        var h = CreateGrain(existingState: ResumableState(AtomicActionPhase.Forward));

        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.Trace, Is.EqualTo(new[] { "fwd" }),
                "a keepalive tick on a started, non-terminal saga must drive it forward");
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Committed));
        });
    }

    [Test]
    public async Task Keepalive_reminder_resumes_a_started_compensating_saga()
    {
        var state = ResumableState(AtomicActionPhase.Compensate);
        state.State.StepStatuses = [AtomicActionStepStatus.ForwardDone];
        state.State.FailedStepIndex = 0;
        state.State.FailureMessage = "boom";

        var h = CreateGrain(existingState: state);

        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.Trace, Is.EqualTo(new[] { "comp" }));
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Compensated));
        });
    }

    [Test]
    public void Keepalive_reminder_contains_a_resume_failure_rather_than_faulting_the_tick()
    {
        // A reminder tick that throws is retried by the Orleans reminder service,
        // so a permanently failing resume would spin forever. The grain logs and
        // swallows instead.
        //
        // A handler whose registration disappeared under an in-flight saga is the
        // realistic trigger: ResolveHandlerForResume fails closed *outside* the
        // per-step try-block precisely so it is not mistaken for an ordinary
        // forward fault, so it propagates all the way out of RunSagaAsync.
        var state = ResumableState(AtomicActionPhase.Forward);
        state.State.Steps = [StampedStep("handler-that-was-unregistered")];

        var h = CreateGrain(existingState: state);

        Assert.DoesNotThrowAsync(() => h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus()));

        Assert.Multiple(() =>
        {
            Assert.That(h.Trace, Is.Empty);
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Forward),
                "the saga stays parked where it was rather than advancing on a failed resume");
        });
    }

    [Test]
    public void Keepalive_reminder_contains_a_version_drift_park()
    {
        // The same containment, reached through the other fail-closed arm: the
        // handler is still registered but its version tag moved, so the saga must
        // not replay a changed effect.
        var state = ResumableState(AtomicActionPhase.Forward);
        state.State.Steps = [new AtomicActionStep
        {
            Kind = AtomicActionStepKind.Custom,
            HandlerId = "h",
            ArgsPayload = [],
            VersionTag = "v0-stale",
        }];

        var h = CreateGrain(existingState: state);

        Assert.DoesNotThrowAsync(() => h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus()));

        Assert.That(h.Trace, Is.Empty, "a changed effect must not be replayed");
    }

    // ----- Tree-write compensation without a recorded pre-image -----

    [Test]
    public async Task Compensating_a_tree_write_step_with_no_recorded_pre_image_is_a_no_op()
    {
        // A tree-write step can be marked ForwardDone while its pre-image list is
        // still empty - the step wrote nothing, or the crash landed between the
        // status write and the pre-image capture. Compensation must then restore
        // nothing rather than fault, otherwise the saga burns its retry budget and
        // parks in CompensationFailed over a step that left no effect behind.
        var state = new FakePersistentState<AtomicActionState>();
        state.State.Phase = AtomicActionPhase.Compensate;
        state.State.Started = true;
        state.State.Steps =
        [
            new AtomicActionStep
            {
                Kind = AtomicActionStepKind.TreeWrite,
                TreeId = "orders",
                Entries = [new AtomicActionEntry { Key = "k", Value = [1] }],
            },
        ];
        state.State.StepStatuses = [AtomicActionStepStatus.ForwardDone];
        state.State.TreeWritePreImages = [];
        state.State.FailedStepIndex = 0;
        state.State.FailureMessage = "boom";

        var h = CreateGrain(existingState: state);

        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.StepStatuses[0], Is.EqualTo(AtomicActionStepStatus.Compensated),
                "the step must settle as compensated, not fault");
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Compensated));
            Assert.That(h.State.State.CompensationRetries, Is.Zero,
                "no retry budget may be consumed by a no-op compensation");
        });
    }

    [Test]
    public async Task Keepalive_reminder_on_an_unstarted_saga_tears_down_instead_of_resuming()
    {
        // Started == false means ExecuteAsync never seeded a plan, so there is
        // nothing to resume; the tick must fall to the teardown branch.
        var reminder = Substitute.For<IGrainReminder>();
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        var h = CreateGrain(
            existingState: ResumableState(AtomicActionPhase.Forward, started: false),
            reminderRegistry: registry);

        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        Assert.That(h.Trace, Is.Empty, "no effect may run for a saga that was never started");
        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public Task Keepalive_reminder_on_a_committed_saga_unregisters_itself()
        => AssertTerminalKeepaliveTearsDown(AtomicActionPhase.Committed);

    [Test]
    public Task Keepalive_reminder_on_a_compensated_saga_unregisters_itself()
        => AssertTerminalKeepaliveTearsDown(AtomicActionPhase.Compensated);

    [Test]
    public Task Keepalive_reminder_on_a_compensation_failed_saga_unregisters_itself()
        => AssertTerminalKeepaliveTearsDown(AtomicActionPhase.CompensationFailed);

    private static async Task AssertTerminalKeepaliveTearsDown(AtomicActionPhase phase)
    {
        var reminder = Substitute.For<IGrainReminder>();
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        var h = CreateGrain(existingState: ResumableState(phase), reminderRegistry: registry);

        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        Assert.That(h.Trace, Is.Empty, "a terminal saga must not re-run any effect on a stray tick");
        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task An_unrelated_reminder_name_is_ignored_entirely()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var h = CreateGrain(existingState: ResumableState(AtomicActionPhase.Forward), reminderRegistry: registry);

        await h.Grain.ReceiveReminder("some-other-grain-reminder", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.Trace, Is.Empty);
            Assert.That(h.State.State.Phase, Is.EqualTo(AtomicActionPhase.Forward),
                "an unrelated reminder must not advance the saga");
        });
        await registry.DidNotReceiveWithAnyArgs().UnregisterReminder(default!, default!);
    }

    // ----- Keepalive registration / unregistration containment -----

    [Test]
    public async Task A_failing_keepalive_registration_does_not_fail_the_saga()
    {
        // The keepalive is a resilience aid, not a correctness precondition: a
        // reminder service that is unavailable must not fail an otherwise valid
        // action.
        var registry = Substitute.For<IReminderRegistry>();
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .ThrowsAsync(new InvalidOperationException("reminder service unavailable"));

        var h = CreateGrain(reminderRegistry: registry);

        var outcome = await h.Grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep()] });

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
            Assert.That(h.Trace, Is.EqualTo(new[] { "fwd" }));
        });
    }

    [Test]
    public async Task A_failing_keepalive_unregistration_does_not_fail_the_saga()
    {
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .ThrowsAsync(new InvalidOperationException("reminder service unavailable"));

        var h = CreateGrain(reminderRegistry: registry);

        var outcome = await h.Grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep()] });

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
    }

    // ----- Plan validation -----

    [Test]
    public void A_tree_write_step_without_a_tree_id_is_rejected()
    {
        var h = CreateGrain();
        var plan = new AtomicActionPlan
        {
            Steps =
            [
                new AtomicActionStep
                {
                    Kind = AtomicActionStepKind.TreeWrite,
                    TreeId = string.Empty,
                    Entries = [new AtomicActionEntry { Key = "k", Value = [1] }],
                },
            ],
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => h.Grain.ExecuteAsync(plan));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("non-empty tree id"));
            Assert.That(h.State.State.Started, Is.False,
                "validation must land before any state is seeded");
        });
    }

    [Test]
    public void A_tree_write_step_without_entries_is_rejected()
    {
        var h = CreateGrain();
        var plan = new AtomicActionPlan
        {
            Steps =
            [
                new AtomicActionStep
                {
                    Kind = AtomicActionStepKind.TreeWrite,
                    TreeId = "orders",
                    Entries = [],
                },
            ],
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => h.Grain.ExecuteAsync(plan));

        Assert.That(ex!.Message, Does.Contain("at least one entry"));
    }

    [Test]
    public void A_custom_step_without_a_handler_id_is_rejected()
    {
        var h = CreateGrain();
        var plan = new AtomicActionPlan
        {
            Steps = [new AtomicActionStep { Kind = AtomicActionStepKind.Custom, HandlerId = string.Empty }],
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => h.Grain.ExecuteAsync(plan));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("non-empty handler id"));
            Assert.That(h.State.State.Started, Is.False);
        });
    }

    // ----- Handler context surface -----

    [Test]
    public async Task The_handler_context_exposes_the_operation_id_and_a_non_cancelled_token()
    {
        string? seenOperationId = null;
        CancellationToken seenToken = default;
        IGrainFactory? seenFactory = null;
        var argsPayload = new byte[] { 7, 8, 9 };
        var seenArgs = ReadOnlyMemory<byte>.Empty;

        var h = CreateGrain(forward: ctx =>
        {
            seenOperationId = ctx.OperationId;
            seenToken = ctx.CancellationToken;
            seenFactory = ctx.GrainFactory;
            seenArgs = ctx.Args;
            return Task.CompletedTask;
        });

        await h.Grain.ExecuteAsync(new AtomicActionPlan
        {
            Steps = [new AtomicActionStep { Kind = AtomicActionStepKind.Custom, HandlerId = "h", ArgsPayload = argsPayload }],
        });

        Assert.Multiple(() =>
        {
            Assert.That(seenOperationId, Is.EqualTo(OperationId));
            Assert.That(seenToken.CanBeCanceled, Is.False,
                "a saga effect must not be cancellable mid-flight; a half-applied step has no compensation record");
            Assert.That(seenFactory, Is.Not.Null);
            Assert.That(seenArgs.ToArray(), Is.EqualTo(argsPayload));
        });
    }
}
