using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1888: <c>AtomicActionState.FailedStepIndex</c>
/// used a <c>-1</c> negative-sentinel initializer over a domain in which <c>0</c>
/// is a legitimate value, so a saga that faulted on its FIRST forward step wrote
/// <c>0</c>, had it omitted by a grain-storage serializer that drops type
/// defaults, and reloaded as <c>-1</c> - which means "no forward fault has
/// occurred". The reloaded saga then reported a clean commit for an action that
/// had actually faulted and compensated.
/// <para>
/// Unlike its sibling in issue 1886 this could not be repaired by deleting the
/// initializer: <c>0</c> is a real step index, not an off state, so removing it
/// would make "unset" indistinguishable from "faulted on step 0". The member is
/// nullable instead, so absent means <see langword="null"/> means unset, and the
/// public <see cref="AtomicActionOutcome.FailedStepIndex"/> contract (<c>-1</c> on
/// a clean commit) is preserved by mapping at that boundary.
/// </para>
/// </summary>
public sealed partial class AtomicActionGrainTests
{
    /// <summary>
    /// The behavioural statement of the defect, in user terms: a saga that faults
    /// on step index <c>0</c> must, after its state has been through a storage
    /// round trip that omits type defaults, still report a forward fault on step
    /// <c>0</c> rather than presenting as a clean commit.
    /// </summary>
    [Test]
    public async Task Saga_that_faulted_on_step_zero_still_reports_the_fault_after_a_default_omitting_reload()
    {
        var trace = new List<string>();
        var catalog = Catalog(
            ("boom", "v1", _ => throw new InvalidOperationException("forward boom on step 0"),
                _ => { trace.Add("boom:C"); return Task.CompletedTask; }),
            ("b", "v1", _ => { trace.Add("b:F"); return Task.CompletedTask; },
                _ => { trace.Add("b:C"); return Task.CompletedTask; }));

        var (grain, state, _) = CreateGrain(catalog);
        var plan = new AtomicActionPlan { Steps = [CustomStep("boom"), CustomStep("b")] };

        var live = await grain.ExecuteAsync(plan);

        // Reload the saga from state that has been through a serializer which
        // omits members equal to default(T) - the production shape this repo's
        // fixtures cannot otherwise reproduce.
        var reloadedState = new FakePersistentState<AtomicActionState>
        {
            State = DefaultOmittingStateRoundTrip.Simulate(state.State),
        };
        var (reloaded, _, _) = CreateGrain(catalog, existingState: reloadedState);

        var outcome = await reloaded.TryGetOutcomeAsync();

        // Asserted together, and with the load-bearing legs last, so an
        // in-memory sanity check cannot throw early and silently disable the
        // reload assertions this test is named for.
        Assert.Multiple(() =>
        {
            Assert.That(live.Status, Is.EqualTo(AtomicActionStatus.Compensated),
                "In memory the saga faulted on step 0 and compensated: no step reached "
                + "ForwardDone, so there was nothing to compensate and it settled Compensated.");
            Assert.That(live.FailedStepIndex, Is.Zero,
                "In memory - before any round trip - the fault is recorded on step 0.");
            Assert.That(trace, Is.Empty,
                "Step 0 faulted before any forward effect completed, so no compensating effect runs.");

            Assert.That(outcome, Is.Not.Null,
                "The reloaded saga is terminal, so its outcome is memoized and returned.");
            Assert.That(outcome!.Value.Status, Is.EqualTo(AtomicActionStatus.Compensated));
            Assert.That(outcome.Value.FailedStepIndex, Is.Zero,
                "RED before issue 1888: FailedStepIndex = 0 is dropped by the omitting serializer "
                + "and the -1 initializer resurrects 'no forward fault has occurred', so the reloaded "
                + "saga reports -1 and a caller branching on FailedStepIndex >= 0 concludes the action "
                + "committed cleanly when it faulted on its first step.");
            Assert.That(outcome.Value.FailureMessage, Does.Contain("forward boom on step 0"),
                "The originating fault message must survive alongside the index it belongs to; a "
                + "message present with an index of -1 is an internally contradictory outcome.");
        });
    }

    /// <summary>
    /// The complementary direction: a saga that never faulted must not acquire a
    /// phantom fault on step <c>0</c> from the repair. A clean commit persists
    /// "unset", which must still read back as the documented <c>-1</c> on the
    /// public outcome rather than as a fault on step zero.
    /// </summary>
    [Test]
    public async Task Saga_that_committed_cleanly_still_reports_no_fault_after_a_default_omitting_reload()
    {
        var catalog = Catalog(
            ("a", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask),
            ("b", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));

        var (grain, state, _) = CreateGrain(catalog);
        var plan = new AtomicActionPlan { Steps = [CustomStep("a"), CustomStep("b")] };

        await grain.ExecuteAsync(plan);

        var reloadedState = new FakePersistentState<AtomicActionState>
        {
            State = DefaultOmittingStateRoundTrip.Simulate(state.State),
        };
        var (reloaded, _, _) = CreateGrain(catalog, existingState: reloadedState);

        var outcome = await reloaded.TryGetOutcomeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.Not.Null);
            Assert.That(outcome!.Value.Status, Is.EqualTo(AtomicActionStatus.Committed));
            Assert.That(outcome.Value.FailedStepIndex, Is.EqualTo(-1),
                "An unset FailedStepIndex must continue to surface as the documented -1 on the "
                + "public outcome, so making the persisted member nullable is not observable to a caller.");
            Assert.That(outcome.Value.FailureMessage, Is.Null);
        });
    }

    /// <summary>
    /// The persisted member itself, asserted directly rather than through the
    /// outcome projection, so the coverage cannot be satisfied by the boundary
    /// mapping alone: what a saga wrote for step <c>0</c> must be what it reads
    /// back, and an un-faulted saga must persist "unset" rather than a real index.
    /// </summary>
    [Test]
    public async Task FailedStepIndex_persists_step_zero_and_unset_distinguishably_across_a_default_omitting_reload()
    {
        var catalog = Catalog(
            ("boom", "v1", _ => throw new InvalidOperationException("boom"), _ => Task.CompletedTask),
            ("ok", "v1", _ => Task.CompletedTask, _ => Task.CompletedTask));

        var (faulting, faultingState, _) = CreateGrain(catalog);
        await faulting.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("boom")] });

        var (clean, cleanState, _) = CreateGrain(catalog);
        await clean.ExecuteAsync(new AtomicActionPlan { Steps = [CustomStep("ok")] });

        var faultingReloaded = DefaultOmittingStateRoundTrip.Simulate(faultingState.State);
        var cleanReloaded = DefaultOmittingStateRoundTrip.Simulate(cleanState.State);

        Assert.Multiple(() =>
        {
            Assert.That(faultingReloaded.FailedStepIndex, Is.Zero,
                "A fault on step 0 must survive the round trip as 0.");
            Assert.That(cleanReloaded.FailedStepIndex, Is.Null,
                "An un-faulted saga must read back as unset, which is what makes 'faulted on step 0' "
                + "and 'never faulted' two distinguishable states rather than one.");
        });
    }
}
