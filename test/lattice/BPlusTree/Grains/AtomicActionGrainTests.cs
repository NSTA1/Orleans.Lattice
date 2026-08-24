using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Fast, runtime-free unit tests for <see cref="AtomicActionGrain"/> (issue #1609).
/// The grain is constructed directly with substitute Orleans seams and a
/// <see cref="FakePersistentState{T}"/>, and driven with a real
/// <see cref="AtomicActionCatalog"/> of delegate handlers whose forward /
/// compensate effects append to a shared trace, so the saga's step-sequencing,
/// reverse-order compensation, crash-resume, and fail-closed behaviours are
/// asserted deterministically without a cluster. The end-to-end tree-write and
/// live-runtime paths are covered by the integration fixture.
/// </summary>
[TestFixture]
public sealed class AtomicActionGrainTests
{
    private const string OperationId = "op-1609";

    private static AtomicActionCatalog Catalog(params (string Id, string Tag, Func<IAtomicActionContext, Task> Fwd, Func<IAtomicActionContext, Task> Comp)[] handlers)
    {
        var map = new Dictionary<string, AtomicActionHandlerRegistration>(StringComparer.Ordinal);
        foreach (var (id, tag, fwd, comp) in handlers)
        {
            map[id] = new AtomicActionHandlerRegistration(
                new DelegateAtomicActionHandler(id, tag, fwd, comp), tag);
        }

        return new AtomicActionCatalog(map);
    }

    private static (AtomicActionGrain Grain, FakePersistentState<AtomicActionState> State, IGrainFactory Factory) CreateGrain(
        IAtomicActionCatalog catalog,
        FakePersistentState<AtomicActionState>? existingState = null,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-action", OperationId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();

        var opts = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<AtomicActionState>();

        var grain = new AtomicActionGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            catalog,
            new LoggerFactory().CreateLogger<AtomicActionGrain>(),
            state);

        return (grain, state, grainFactory);
    }

    private static AtomicActionStep CustomStep(string handlerId, byte[]? args = null) => new()
    {
        Kind = AtomicActionStepKind.Custom,
        HandlerId = handlerId,
        ArgsPayload = args ?? [],
    };

    // --- Happy path ---

    [Test]
    public async Task ExecuteAsync_multi_step_plan_commits_and_runs_every_forward_in_order()
    {
        var trace = new List<string>();
        var catalog = Catalog(
            ("a", "v1", _ => { trace.Add("a:F"); return Task.CompletedTask; }, _ => { trace.Add("a:C"); return Task.CompletedTask; }),
            ("b", "v1", _ => { trace.Add("b:F"); return Task.CompletedTask; }, _ => { trace.Add("b:C"); return Task.CompletedTask; }));
        var (grain, _, _) = CreateGrain(catalog);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("b")] };
        var outcome = await grain.ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(outcome.FailedStepIndex, Is.EqualTo(-1));
        Assert.That(trace, Is.EqualTo(new[] { "a:F", "b:F" }));
    }

    // --- Forward fault triggers reverse-order compensation ---

    [Test]
    public async Task ExecuteAsync_forward_fault_compensates_completed_steps_in_reverse_order()
    {
        var trace = new List<string>();
        var catalog = Catalog(
            ("a", "v1", _ => { trace.Add("a:F"); return Task.CompletedTask; }, _ => { trace.Add("a:C"); return Task.CompletedTask; }),
            ("b", "v1", _ => { trace.Add("b:F"); return Task.CompletedTask; }, _ => { trace.Add("b:C"); return Task.CompletedTask; }),
            ("boom", "v1", _ => throw new InvalidOperationException("forward boom"), _ => { trace.Add("boom:C"); return Task.CompletedTask; }));
        var (grain, _, _) = CreateGrain(catalog);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("b"), CustomStep("boom")] };
        var outcome = await grain.ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Compensated));
        Assert.That(outcome.FailedStepIndex, Is.EqualTo(2));
        Assert.That(outcome.FailureMessage, Does.Contain("forward boom"));
        // a, b committed; boom faulted (never ForwardDone so never compensated);
        // compensation runs b then a, strict reverse.
        Assert.That(trace, Is.EqualTo(new[] { "a:F", "b:F", "b:C", "a:C" }));
    }

    // --- Compensation fault parks in CompensationFailed and throws ---

    [Test]
    public void ExecuteAsync_compensation_fault_parks_in_compensation_failed_and_throws()
    {
        var catalog = Catalog(
            ("a", "v1", _ => Task.CompletedTask, _ => throw new InvalidOperationException("comp boom")),
            ("boom", "v1", _ => throw new InvalidOperationException("forward boom"), _ => Task.CompletedTask));
        var (grain, state, _) = CreateGrain(catalog);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("boom")] };

        var ex = Assert.ThrowsAsync<CompensationFailedException>(() => grain.ExecuteAsync(plan));
        Assert.That(ex!.StepIndex, Is.EqualTo(0));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicActionPhase.CompensationFailed));
    }

    // --- Idempotent re-entry ---

    [Test]
    public async Task ExecuteAsync_repeat_after_terminal_returns_memoized_outcome_without_rerunning()
    {
        var forwardCount = 0;
        var catalog = Catalog(
            ("a", "v1", _ => { forwardCount++; return Task.CompletedTask; }, _ => Task.CompletedTask));
        var (grain, _, _) = CreateGrain(catalog);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a")] };
        var first = await grain.ExecuteAsync(plan);
        var second = await grain.ExecuteAsync(plan);

        Assert.That(first.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(second, Is.EqualTo(first));
        Assert.That(forwardCount, Is.EqualTo(1), "the forward effect must not run again on re-entry");
    }

    [Test]
    public async Task ExecuteAsync_reentry_with_a_different_plan_is_rejected()
    {
        var catalog = Catalog(
            ("a", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask),
            ("b", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));
        var (grain, _, _) = CreateGrain(catalog);

        await grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("a")] });

        Assert.That(
            () => grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("b")] }),
            Throws.ArgumentException);
    }

    [Test]
    public async Task TryGetOutcomeAsync_returns_null_before_run_and_outcome_after()
    {
        var catalog = Catalog(("a", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));
        var (grain, _, _) = CreateGrain(catalog);

        Assert.That(await grain.TryGetOutcomeAsync(), Is.Null);

        await grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("a")] });

        var outcome = await grain.TryGetOutcomeAsync();
        Assert.That(outcome, Is.Not.Null);
        Assert.That(outcome!.Value.Status, Is.EqualTo(AtomicActionStatus.Committed));
    }

    // --- Fail-closed handler resolution ---

    [Test]
    public void ExecuteAsync_unregistered_handler_id_fails_closed()
    {
        var (grain, _, _) = CreateGrain(Catalog());

        Assert.That(
            () => grain.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("nope")] }),
            Throws.InstanceOf<AtomicActionHandlerNotRegisteredException>());
    }

    // --- Validation ---

    [Test]
    public void ExecuteAsync_null_plan_throws()
    {
        var (grain, _, _) = CreateGrain(Catalog());
        Assert.That(() => grain.ExecuteAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ExecuteAsync_empty_plan_throws()
    {
        var (grain, _, _) = CreateGrain(Catalog());
        Assert.That(
            () => grain.ExecuteAsync(new AtomicActionPlan { Steps = [] }),
            Throws.ArgumentException);
    }

    [Test]
    public void ExecuteAsync_plan_exceeding_max_steps_throws()
    {
        var catalog = Catalog(("a", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));
        var options = new LatticeOptions { MaxAtomicActionSteps = 2 };
        var (grain, _, _) = CreateGrain(catalog, options: options);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("a"), CustomStep("a")] };
        Assert.That(() => grain.ExecuteAsync(plan), Throws.ArgumentException);
    }

    [Test]
    public void ExecuteAsync_args_payload_over_limit_is_rejected()
    {
        var catalog = Catalog(("a", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));
        var options = new LatticeOptions { MaxAtomicActionArgsBytes = 8 };
        var (grain, _, _) = CreateGrain(catalog, options: options);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a", new byte[9])] };
        Assert.That(() => grain.ExecuteAsync(plan), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // --- Crash-resume from persisted state ---

    [Test]
    public async Task ExecuteAsync_resume_from_partial_forward_runs_only_the_pending_step()
    {
        var trace = new List<string>();
        var catalog = Catalog(
            ("a", "v1", _ => { trace.Add("a:F"); return Task.CompletedTask; }, _ => Task.CompletedTask),
            ("b", "v1", _ => { trace.Add("b:F"); return Task.CompletedTask; }, _ => Task.CompletedTask));

        // Seed persisted state as if the grain crashed after step 0 committed but
        // before step 1: Started, Forward phase, [ForwardDone, Pending].
        var seeded = new FakePersistentState<AtomicActionState>
        {
            State =
            {
                Started = true,
                Phase = AtomicActionPhase.Forward,
                Steps = [Persisted("a"), Persisted("b")],
                StepStatuses = [AtomicActionStepStatus.ForwardDone, AtomicActionStepStatus.Pending],
                FailedStepIndex = -1,
                PlanFingerprint = null,
            },
        };
        var (grain, _, _) = CreateGrain(catalog, seeded);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("b")] };
        var outcome = await grain.ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(trace, Is.EqualTo(new[] { "b:F" }), "the already-committed step 0 must not re-run");
    }

    // --- Version-tag change parks a resumed saga ---

    [Test]
    public void ExecuteAsync_resume_with_changed_handler_version_tag_parks()
    {
        // The catalog now reports v2 for handler 'a', but the in-flight saga step
        // was stamped v1 when it started - a redeploy changed the effect contract
        // mid-saga, so the resume must refuse to replay a changed effect.
        var catalog = Catalog(("a", "v2", _ => Task.CompletedTask, _ => Task.CompletedTask));

        var seeded = new FakePersistentState<AtomicActionState>
        {
            State =
            {
                Started = true,
                Phase = AtomicActionPhase.Forward,
                Steps = [PersistedWithTag("a", "v1")],
                StepStatuses = [AtomicActionStepStatus.Pending],
                FailedStepIndex = -1,
                PlanFingerprint = null,
            },
        };
        var (grain, _, _) = CreateGrain(catalog, seeded);

        var plan = new AtomicActionPlan { Steps = [CustomStep("a")] };
        Assert.That(() => grain.ExecuteAsync(plan), Throws.InstanceOf<InvalidOperationException>());
    }

    private static AtomicActionStep Persisted(string handlerId) => PersistedWithTag(handlerId, "v1");

    private static AtomicActionStep PersistedWithTag(string handlerId, string tag) => new()
    {
        Kind = AtomicActionStepKind.Custom,
        HandlerId = handlerId,
        ArgsPayload = [],
        VersionTag = tag,
    };
}
