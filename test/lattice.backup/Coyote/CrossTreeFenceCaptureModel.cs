using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// Which rule of the cross-tree fence window a run of
/// <see cref="CrossTreeFenceCaptureModel"/> takes.
/// </summary>
internal enum CrossTreeFenceGuard
{
    /// <summary>The production window: <see cref="CrossTreeFenceWindow"/> as shipped.</summary>
    Production,

    /// <summary>The post-capture re-observation ignores the registration epoch.</summary>
    IgnoreEpoch,

    /// <summary>The drain gate passes without waiting for the in-flight count to drain.</summary>
    SkipDrain,

    /// <summary>
    /// Anti-vacuity witness: the production window, asserting instead that an
    /// accepted set never holds the saga committed. Exploration must refute it,
    /// which proves a committed saga is captured on some explored schedule.
    /// </summary>
    WitnessCommittedCaptured,
}

/// <summary>
/// A Coyote model of a cross-tree-consistent backup set captured over two trees
/// while one cross-tree atomic saga runs, driving the <b>production</b>
/// <see cref="CrossTreeFenceWindow"/> - the drain gate and the post-capture
/// re-observation <see cref="LatticeBackupCaptureService"/> routes through it -
/// under schedule exploration. It is the implementation-level companion of
/// <c>spec/backup/BackupCapture.tla</c>'s <c>SetSagaConsistent</c>.
/// <para>
/// The saga registers its decision authority on each tree (a delegation row and
/// an epoch bump each), records its coordinator decision, then finalizes each
/// tree (dropping that tree's row) and broadcasts each tree's terminal after its
/// finalize, in any interleaving. It may start before the capture, during the
/// drain, or inside the capture window. The capture drains, records each tree's
/// epoch at the drained moment, captures tree 0 then tree 1, and re-observes; an
/// unstable attempt is retried once, then the capture fails.
/// </para>
/// <para>
/// Each tree holds one key of the saga, so a member capture is one instant. A
/// still-pending bucket is resolved against the tree's LOCAL decision record, as
/// the #4485 fix specifies. Production today serves it pre-saga, which tears even
/// a set this window accepts, so this model checks the window under the intended
/// per-tree capture and leaves the per-tree defect to #4485.
/// </para>
/// <para>
/// No gate input is pinned: the saga's decision, finalize order, broadcast order
/// and start time, and every interleaving with the capture, are drawn from the
/// runtime. Every per-iteration value is a local of <see cref="Run"/>.
/// </para>
/// </summary>
internal sealed class CrossTreeFenceCaptureModel(CrossTreeFenceGuard guard, bool sagaRuns = true) : ICoyoteModel
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
        Drain,
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

        var phase = CapturePhase.Drain;
        var attempt = 1;
        var epochAtDrain = new long[Trees];
        var images = new Image[Trees];
        var captured = 0;

        var steps = 0;
        while (phase is not (CapturePhase.Accepted or CapturePhase.Failed) && steps++ < 200)
        {
            var sagaCanStep = sagaRuns && SagaCanStep();
            var captureCanStep = CaptureCanStep();
            if (!sagaCanStep && !captureCanStep)
            {
                // Nothing else can move the in-flight count: the drain times out.
                phase = CapturePhase.Failed;
                break;
            }

            if (sagaCanStep && (!captureCanStep || runtime.RandomBoolean()))
            {
                StepSaga();
                continue;
            }

            switch (phase)
            {
                case CapturePhase.Drain:
                    epoch.CopyTo(epochAtDrain, 0);
                    phase = CapturePhase.Capture;
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
                        var baseline = guard == CrossTreeFenceGuard.IgnoreEpoch ? epoch[t] : epochAtDrain[t];
                        stable &= CrossTreeFenceWindow.IsStable(baseline, epoch[t], inFlight[t]);
                    }

                    if (stable)
                    {
                        phase = CapturePhase.Accepted;
                    }
                    else if (attempt < MaxAttempts)
                    {
                        attempt++;
                        captured = 0;
                        images[0] = images[1] = Image.None;
                        phase = CapturePhase.Drain;
                    }
                    else
                    {
                        phase = CapturePhase.Failed;
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

        if (guard == CrossTreeFenceGuard.WitnessCommittedCaptured)
        {
            Specification.Assert(
                images[0] != Image.Post && images[1] != Image.Post,
                "witness: an accepted set captured the committed saga");
            return;
        }

        Specification.Assert(
            !(images[0] == Image.Post && images[1] == Image.Pre)
            && !(images[0] == Image.Pre && images[1] == Image.Post),
            $"an accepted cross-tree backup set holds the saga torn: tree0={images[0]}, tree1={images[1]} ({guard})");

        bool CaptureCanStep() => phase switch
        {
            CapturePhase.Drain => guard == CrossTreeFenceGuard.SkipDrain
                || CrossTreeFenceWindow.IsDrained(inFlight[0] + inFlight[1]),
            CapturePhase.Capture or CapturePhase.Reobserve => true,
            _ => false,
        };

        bool SagaCanStep()
        {
            if (!registered[0] || !registered[1])
            {
                return true;
            }

            if (decision == Outcome.None)
            {
                return true;
            }

            for (var t = 0; t < Trees; t++)
            {
                if (local[t] == Outcome.None || terminal[t] == Outcome.None)
                {
                    return true;
                }
            }

            return false;
        }

        void StepSaga()
        {
            for (var t = 0; t < Trees; t++)
            {
                if (!registered[t])
                {
                    registered[t] = true;
                    inFlight[t] = 1;
                    epoch[t]++;
                    return;
                }
            }

            if (decision == Outcome.None)
            {
                decision = runtime.RandomBoolean() ? Outcome.Committed : Outcome.Aborted;
                return;
            }

            // Finalize or broadcast on a tree the runtime picks, in any order,
            // a tree's broadcast only after its finalize.
            var first = runtime.RandomBoolean() ? 0 : 1;
            for (var i = 0; i < Trees; i++)
            {
                var t = (first + i) % Trees;
                if (local[t] == Outcome.None)
                {
                    local[t] = decision;
                    inFlight[t] = 0;
                    return;
                }

                if (terminal[t] == Outcome.None && runtime.RandomBoolean())
                {
                    terminal[t] = local[t];
                    return;
                }
            }

            for (var t = 0; t < Trees; t++)
            {
                if (terminal[t] == Outcome.None)
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
