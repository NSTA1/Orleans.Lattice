using Orleans.Lattice.Testing;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// What the ANN key walk does with a WAL replay permit REFUSAL (issue #3284),
/// as distinct from what the leaf does when it refuses.
/// <para>
/// <b>Why this fixture exists, stated as the gap it closes.</b> The leaf-level
/// fixtures prove an activation past the admission bound throws
/// <see cref="LatticeSaturatedException"/>. Nothing in them says what the walk
/// then does with it, and the walk is where the damage was: the open classifies
/// itself <c>Bulk</c> for its whole fan-out, so under exactly the saturation this
/// change targets the refusal is raised INTO the walk. It is not an
/// <see cref="OperationCanceledException"/>, so it misses the deferral clause and
/// would land on the general fault arm - recorded as
/// <c>outcome="faulted"</c> and rethrown.
/// </para>
/// <para>
/// <b>That misattribution destroys the only clean falsifier for the fix.</b>
/// <c>repocontext.ann.index.load_total{outcome="faulted"}</c> would rise BECAUSE
/// the admission bound started working, so after a redeploy a rise, a fall, and
/// no change are each consistent with "the fix worked" and with "the fix made it
/// worse". These counters are process-scoped and reset at the deploy boundary, so
/// there is exactly one clean reading available and no way to reconstruct it
/// afterwards. It is the same argument the leaf's own reason-value split already
/// won, left unapplied one layer up.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    /// <summary>
    /// Builds a handle over a store armed to refuse, and returns both so a test
    /// can assert on the walk's outcome and on the harness having actually run.
    /// </summary>
    private static async Task<(BlockingScanStore Store, string Prefix)> RefusingStoreAsync(
        int serveBeforeRefusing)
    {
        var store = await SeededStoreAsync();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);
        store.RefuseAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), serveBeforeRefusing);
        return (store, prefix);
    }

    [Test]
    public async Task A_refused_walk_is_not_counted_as_a_fault()
    {
        // THE DEFECT, STATED AS A TEST. Before the refusal arm this recorded
        // Faulted and rethrew, and no fixture anywhere covered a refusal reaching
        // the walk at all.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        await handle.AdvanceAsync(Ct);

        Assert.That(store.Refusals, Is.GreaterThan(0),
            "instrument validation: the walk must actually have reached the refusal, or every "
            + "assertion below passes by observing a walk that never ran");

        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Faulted, Is.EqualTo(0),
                "A REFUSAL IS THE BOUND WORKING, NOT A FAULT. Counting it here makes the faulted arm "
                + "rise because the fix started working, so a post-deploy rise, fall, and no-change "
                + "are all consistent with the fix having worked AND with it having made things worse - "
                + "which destroys the measurement this change has to be judged by.");

            Assert.That(snapshot.Refused, Is.EqualTo(1),
                "The refusal must be ATTRIBUTABLE, not merely benign. Folding it into the deferral arm "
                + "would keep the faulted arm clean while making refusals invisible, which is quieter "
                + "but no more informative: a deferral means this walk ran and needs longer, a refusal "
                + "means it never started because the silo is saturated, and the remedies differ.");

            Assert.That(snapshot.Deferred, Is.EqualTo(0),
                "The slice budget did not expire here - the walk was turned away before it could spend "
                + "one - so the deferral arm must not move.");
        });
    }

    [Test]
    public async Task A_refused_walk_yields_rather_than_propagating_the_refusal()
    {
        // The caller contract. A refusal is retryable back-pressure, so a single
        // refusal must leave the handle able to try again on the next tick rather
        // than tearing the coordinator's turn down.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        Assert.DoesNotThrowAsync(async () => await handle.AdvanceAsync(Ct),
            "a single refusal is back-pressure, and the walk banks what it read and resumes next tick");

        Assert.That(handle.IsServing, Is.False,
            "the walk was refused, so it holds no index to serve from");

        // The regime clears, which is what saturation does, and the walk completes
        // from the progress it banked rather than restarting.
        store.StopRefusing();
        await handle.EnsureBuiltAsync(Ct);

        Assert.That(handle.IsServing, Is.True,
            "once the silo stops refusing, the walk must complete - a refusal that poisoned the handle "
            + "would turn transient back-pressure into a permanent outage");
    }

    [Test]
    public async Task A_permanently_refused_walk_escalates_rather_than_retrying_for_ever()
    {
        // THE TRAP IN THE OBVIOUS FIX, AND THE REASON THE REFUSAL SHARES THE
        // DEFERRAL'S COUNTER. EnsureBuiltAsync loops until the handle serves. A
        // walk refused on every attempt banks nothing on every attempt, so an arm
        // that returned null without counting toward the empty-slice escalation
        // would spin for ever - reintroducing, inside the fix for the bound, the
        // exact unbounded retry issue #3130 removed.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // One short of the escalation, so the assertion below is about the LAST
        // attempt rather than about an arbitrary one.
        for (var attempt = 1; attempt < MaxEmptyOpenDeferrals; attempt++)
        {
            await handle.AdvanceAsync(Ct);
        }

        Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await handle.AdvanceAsync(Ct),
            $"after {MaxEmptyOpenDeferrals} consecutive refusals that banked nothing the open "
            + "must escalate. The ORIGINAL saturation type is rethrown rather than converted, so any "
            + "upstream backoff keyed to it still applies.");

        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Refused, Is.EqualTo(MaxEmptyOpenDeferrals),
                "one arm per attempt, on the escalating attempt as much as the yielding ones");

            Assert.That(snapshot.Faulted, Is.EqualTo(0),
                "EVEN THE ESCALATION MUST NOT TOUCH THE FAULTED ARM. Escalating on the third refusal "
                + "while recording it as a fault would leave refusals polluting the falsifier at a "
                + "third of the rate, which is a quieter version of the same defect rather than a fix.");
        });
    }

    [Test]
    public async Task A_refusal_that_banked_progress_does_not_count_toward_escalation()
    {
        // The counter is PRESENT-TENSE, and it has to be: a walk that advances on
        // every attempt and is refused on every attempt is converging, not wedged.
        // A lifetime tally would escalate it on the third attempt and turn a
        // healthy sliced open over a busy silo into a hard failure.
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);
        var keyMap = VectorIndexStorageKeys.KeyMapPrefix(prefix);
        using var reporter = new RepoContextAnnIndexLoadReporter();

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // Each attempt serves one more mapping than the last before being refused,
        // so every attempt banks progress.
        for (var attempt = 1; attempt <= MaxEmptyOpenDeferrals + 2; attempt++)
        {
            store.RefuseAfter(keyMap, attempt);
            await handle.AdvanceAsync(Ct);
        }

        Assert.That(store.Refusals, Is.GreaterThanOrEqualTo(MaxEmptyOpenDeferrals + 2),
            "instrument validation: every attempt must have been refused, or this asserts nothing "
            + "about consecutive refusals");

        Assert.That(reporter.Snapshot().Faulted, Is.EqualTo(0),
            "A WALK THAT ADVANCES EVERY ATTEMPT IS CONVERGING. Escalating it because the refusals were "
            + "consecutive would fail a healthy sliced open over a merely busy silo.");
    }
}
