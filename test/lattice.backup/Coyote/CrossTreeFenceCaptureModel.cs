using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// The rules of the cross-tree fence window a run of
/// <see cref="CrossTreeFenceCaptureModel"/> leaves out. Every rule left in is
/// decided by the production <see cref="CrossTreeFenceWindow"/>.
/// </summary>
[Flags]
internal enum CrossTreeFenceRulesRemoved
{
    /// <summary>The production window: every rule in place.</summary>
    None = 0,

    /// <summary>The drain gate passes without waiting for the in-flight count to drain.</summary>
    Drain = 1,

    /// <summary>The gated in-flight re-check (<see cref="CrossTreeFenceWindow.IsRecheckClean"/>) always passes.</summary>
    Recheck = 2,

    /// <summary>The post-capture re-observation ignores the registration epoch.</summary>
    ReobserveEpoch = 4,

    /// <summary>The post-capture re-observation ignores the in-flight count.</summary>
    ReobserveInFlight = 8,
}

/// <summary>
/// A Coyote model of a cross-tree-consistent backup set captured over two trees
/// while one cross-tree atomic saga runs, following the shipped #4485 design of
/// <see cref="LatticeBackupCaptureService"/>'s fenced set capture and driving
/// the <b>production</b> <see cref="CrossTreeFenceWindow"/> for each of its
/// rules: the drain gate (<see cref="CrossTreeFenceWindow.IsDrained"/>), the
/// gated re-check (<see cref="CrossTreeFenceWindow.IsRecheckClean"/>) and the
/// post-capture re-observation (<see cref="CrossTreeFenceWindow.IsStable"/>,
/// both its epoch and its in-flight clause). It is the implementation-level
/// companion of <c>spec/backup/BackupCapture.tla</c>'s <c>SetSagaConsistent</c>.
/// <para>
/// The capture fences both trees (a new delegation registration is refused, and
/// a refused saga aborts), drains, takes the gate (decisions, so a tree's
/// finalize, are refused until release), re-checks, captures tree 0 then tree
/// 1, and re-observes; a refused attempt is retried once, then the capture
/// fails. When <c>lapse</c> is set the fence may be lost once between the drain
/// and the gate - the hold a registry reactivation drops, and the only way a
/// registration enters the window (<c>LatticeBackupSetCaptureHoldLossTests</c>).
/// </para>
/// <para>
/// The saga registers its decision authority on each tree (a delegation row and
/// an epoch bump each), records its coordinator decision, then finalizes each
/// tree (dropping that tree's row) and broadcasts each tree's terminal after its
/// finalize, in any interleaving with the capture. Each tree holds one key of
/// the saga, so a member capture is one instant, and a still-pending bucket is
/// resolved against the tree's LOCAL decision, as the #4485 fix does
/// (<c>SnapshotProjectionFolder.ResolvePendingAgainst</c>).
/// </para>
/// <para>
/// Under this design the window's rules are mutually redundant: with the lapse,
/// any one of the re-check, the epoch clause and the in-flight clause alone
/// refuses a torn attempt, and without it the drain alone does. The tests pin
/// each rule by an arm in which it is the only one left standing, so perturbing
/// that rule in the production core turns its arm red.
/// </para>
/// <para>
/// No gate input is pinned: the saga's decision, finalize order, broadcast order
/// and start time, the lapse, and every interleaving with the capture, are drawn
/// from the runtime. Every per-iteration value is a local of <see cref="Run"/>.
/// </para>
/// </summary>
internal sealed class CrossTreeFenceCaptureModel(
    CrossTreeFenceRulesRemoved removed,
    bool lapse = true,
    bool sagaRuns = true,
    bool witnessCommittedCaptured = false) : ICoyoteModel
{
    private const int Trees = 2;
    private const int MaxAttempts = 2;

    private enum Image : byte
    {
        None,
        Pre,
        Post,
    }

    private enum Outcome : byte
    {
        None,
        Committed,
        Aborted,
    }

    private enum CapturePhase : byte
    {
        Fence,
        Drain,
        Gate,
        Recheck,
        Capture,
        Reobserve,
        Accepted,
        Failed,
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var epoch = new long[Trees];
        var inFlight = new int[Trees];
        var local = new Outcome[Trees];
        var terminal = new Outcome[Trees];
        var registered = new bool[Trees];
        var decision = Outcome.None;

        var phase = CapturePhase.Fence;
        var attempt = 1;
        var fenced = false;
        var gated = false;
        var lapseUsed = !lapse;
        var epochAtDrain = new long[Trees];
        var images = new Image[Trees];
        var captured = 0;

        var steps = 0;
        while (phase is not (CapturePhase.Accepted or CapturePhase.Failed) && steps++ < 300)
        {
            var sagaCanStep = sagaRuns && SagaCanStep();
            var lapseCanStep = !lapseUsed && phase == CapturePhase.Gate && fenced;
            var captureCanStep = CaptureCanStep();
            if (!sagaCanStep && !captureCanStep)
            {
                // Nothing else can move the in-flight count: the drain times out.
                phase = CapturePhase.Failed;
                break;
            }

            if (lapseCanStep && runtime.RandomBoolean())
            {
                // A registry reactivation drops the fence hold between the drain
                // and the gate; the gate's acquire then takes a fresh hold.
                lapseUsed = true;
                fenced = false;
                continue;
            }

            if (sagaCanStep && (!captureCanStep || runtime.RandomBoolean()))
            {
                StepSaga();
                continue;
            }

            switch (phase)
            {
                case CapturePhase.Fence:
                    fenced = true;
                    phase = CapturePhase.Drain;
                    break;

                case CapturePhase.Drain:
                    epoch.CopyTo(epochAtDrain, 0);
                    phase = CapturePhase.Gate;
                    break;

                case CapturePhase.Gate:
                    fenced = true;
                    gated = true;
                    phase = CapturePhase.Recheck;
                    break;

                case CapturePhase.Recheck:
                    var clean = true;
                    for (var t = 0; t < Trees && !removed.HasFlag(CrossTreeFenceRulesRemoved.Recheck); t++)
                    {
                        clean &= CrossTreeFenceWindow.IsRecheckClean(inFlight[t], 0);
                    }

                    if (clean)
                    {
                        phase = CapturePhase.Capture;
                    }
                    else
                    {
                        RetryOrFail();
                    }

                    break;

                case CapturePhase.Capture:
                    images[captured] = CaptureMember(captured);
                    if (++captured == Trees)
                    {
                        phase = CapturePhase.Reobserve;
                    }

                    break;

                case CapturePhase.Reobserve:
                    var stable = true;
                    for (var t = 0; t < Trees; t++)
                    {
                        var baseline = removed.HasFlag(CrossTreeFenceRulesRemoved.ReobserveEpoch) ? epoch[t] : epochAtDrain[t];
                        var live = removed.HasFlag(CrossTreeFenceRulesRemoved.ReobserveInFlight) ? 0 : inFlight[t];
                        stable &= CrossTreeFenceWindow.IsStable(baseline, epoch[t], live);
                    }

                    if (stable)
                    {
                        phase = CapturePhase.Accepted;
                        fenced = gated = false;
                    }
                    else
                    {
                        RetryOrFail();
                    }

                    break;
            }
        }

        if (!sagaRuns)
        {
            // No-regression arm: with no saga in flight the window never refuses.
            Specification.Assert(phase == CapturePhase.Accepted, $"a quiet set was not accepted: {phase}");
            return;
        }

        if (phase != CapturePhase.Accepted)
        {
            return;
        }

        if (witnessCommittedCaptured)
        {
            Specification.Assert(
                images[0] != Image.Post && images[1] != Image.Post,
                "witness: an accepted set captured the committed saga");
            return;
        }

        Specification.Assert(
            !(images[0] == Image.Post && images[1] == Image.Pre)
            && !(images[0] == Image.Pre && images[1] == Image.Post),
            $"an accepted cross-tree backup set holds the saga torn: tree0={images[0]}, tree1={images[1]} (removed: {removed}, lapse: {lapse})");

        void RetryOrFail()
        {
            fenced = gated = false;
            if (attempt < MaxAttempts)
            {
                attempt++;
                captured = 0;
                images[0] = images[1] = Image.None;
                phase = CapturePhase.Fence;
            }
            else
            {
                phase = CapturePhase.Failed;
            }
        }

        bool CaptureCanStep() => phase switch
        {
            CapturePhase.Drain => removed.HasFlag(CrossTreeFenceRulesRemoved.Drain)
                || CrossTreeFenceWindow.IsDrained(inFlight[0] + inFlight[1]),
            CapturePhase.Fence or CapturePhase.Gate or CapturePhase.Recheck
                or CapturePhase.Capture or CapturePhase.Reobserve => true,
            _ => false,
        };

        bool SagaCanStep()
        {
            if (decision == Outcome.None)
            {
                return true;
            }

            for (var t = 0; t < Trees; t++)
            {
                if (registered[t] && local[t] == Outcome.None && !gated)
                {
                    return true;
                }

                if (local[t] != Outcome.None && terminal[t] == Outcome.None)
                {
                    return true;
                }
            }

            return false;
        }

        void StepSaga()
        {
            if (decision == Outcome.None)
            {
                for (var t = 0; t < Trees; t++)
                {
                    if (!registered[t])
                    {
                        if (fenced)
                        {
                            // The fence refuses the registration and the refusal is
                            // not retried: the sub-saga votes Failed and its
                            // coordinator aborts.
                            decision = Outcome.Aborted;
                            return;
                        }

                        registered[t] = true;
                        inFlight[t] = 1;
                        epoch[t]++;
                        return;
                    }
                }

                decision = runtime.RandomBoolean() ? Outcome.Committed : Outcome.Aborted;
                return;
            }

            // Finalize (refused under the gate) or broadcast on a tree the runtime
            // picks, in any order, a tree's broadcast only after its finalize.
            var first = runtime.RandomBoolean() ? 0 : 1;
            for (var i = 0; i < Trees; i++)
            {
                var t = (first + i) % Trees;
                if (registered[t] && local[t] == Outcome.None && !gated)
                {
                    local[t] = decision;
                    inFlight[t] = 0;
                    return;
                }

                if (local[t] != Outcome.None && terminal[t] == Outcome.None)
                {
                    terminal[t] = local[t];
                    return;
                }
            }
        }

        Image CaptureMember(int tree)
        {
            if (!registered[tree])
            {
                return Image.Pre;
            }

            if (terminal[tree] != Outcome.None)
            {
                return terminal[tree] == Outcome.Committed ? Image.Post : Image.Pre;
            }

            return local[tree] == Outcome.Committed ? Image.Post : Image.Pre;
        }
    }
}
