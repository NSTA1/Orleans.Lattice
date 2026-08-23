using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// How a <see cref="WalCommitLogWriterDrainModel"/> caller composes the drain
/// token with its admission wait, so the safety test can prove the token must be
/// observed <em>as part of</em> the wait by removing that composition and
/// asserting Coyote re-finds a parked caller that the drain never releases (a
/// shutdown wedge).
/// </summary>
public enum WalCommitLogWriterDrainMode
{
    /// <summary>
    /// The fix: a caller parks on the admission wait with the drain token in its
    /// wait set, exactly as <c>WalCommitLogWriter</c>'s acquire observes the
    /// per-instance drain CTS alongside the caller's token. However the drain
    /// interleaves, a parked caller is released the moment the token is cancelled.
    /// </summary>
    ObserveDrainTokenInWait,

    /// <summary>
    /// The guard removed: a caller checks the drain token, yields, and only then
    /// parks - without the token in its wait set. A drain that fires in the gap
    /// has already cancelled the token, so the caller parks unaware and is never
    /// woken: a lost-wakeup that wedges the silo on shutdown.
    /// </summary>
    CheckTokenThenWait,
}

/// <summary>
/// A Coyote concurrency model of the <see cref="WalCommitLogWriter"/> shutdown
/// drain releasing parked admission callers, driving the <b>production</b>
/// pre-admission gate (<see cref="WalAdmissionGateCore.IsDispatchRefused"/>) that
/// the writer applies before a caller parks. Because the model executes the same
/// gate decision Orleans runs and mirrors the token-observing wait, a violation
/// Coyote finds is a violation of the real drain path.
/// <para>
/// The scenario interleaves <c>callerCount</c> admission callers against a single
/// drain. The per-partition admission semaphore is modelled at zero spare
/// capacity so every admitted caller must park - isolating the property that the
/// drain, not an acquire, is what releases them. A caller that reaches the gate
/// after the drain flag is up is refused (a terminal, live outcome); a caller
/// that passed the gate before the drain must still be released by the token.
/// </para>
/// <para>
/// The safety property is <b>every caller reaches a terminal state</b> (acquired,
/// refused, or drain-released) once the drain has fired - no caller is parked
/// forever. A lost-wakeup that leaves a caller parked after the drain completed
/// is a shutdown-liveness violation.
/// </para>
/// </summary>
public sealed class WalCommitLogWriterDrainModel : ICoyoteModel
{
    private readonly int _callerCount;
    private readonly WalCommitLogWriterDrainMode _mode;

    // Shared writer state the callers and the drain contend over.
    private bool _isDraining;
    private bool _drainTokenCancelled;

    /// <summary>
    /// Creates the model for <paramref name="callerCount"/> admission callers
    /// racing the drain under the chosen token-observation <paramref name="mode"/>.
    /// </summary>
    public WalCommitLogWriterDrainModel(int callerCount, WalCommitLogWriterDrainMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(callerCount, 1);
        _callerCount = callerCount;
        _mode = mode;
    }

    private enum CallerPhase
    {
        NotStarted,

        // Passed the pre-admission gate; about to park (safe mode) or about to
        // sample the token before parking (guard mode).
        PassedGate,

        // Guard mode only: sampled the token and is about to park on the result.
        SampledToken,

        // Parked on the admission wait, awaiting release.
        Parked,

        // Reached a live terminal outcome (refused, drain-released, or acquired).
        Terminal,
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        _isDraining = false;
        _drainTokenCancelled = false;

        var phase = new CallerPhase[_callerCount];
        var sawTokenAtSample = new bool[_callerCount];
        var drainFired = false;

        // Advance one scheduler-chosen actor by one step per iteration while any
        // can still make progress. Every actor advances monotonically and a parked
        // caller can only progress once the drain has fired, so exploration always
        // terminates - and a caller left non-terminal at exit is a genuine wedge,
        // which the post-loop assertion reports.
        while (true)
        {
            var callerCanProgress = AnyCallerCanProgress(phase, sawTokenAtSample, _drainTokenCancelled);
            var driveDrain = !drainFired && (runtime.RandomBoolean() || !callerCanProgress);

            if (driveDrain)
            {
                // DrainAsync: flip the flag and cancel the token in one step.
                _isDraining = true;
                _drainTokenCancelled = true;
                drainFired = true;
                continue;
            }

            if (!callerCanProgress)
            {
                // Drain has fired and no caller can advance: the run is settled.
                break;
            }

            var c = SelectProgressableCaller(phase, sawTokenAtSample, _drainTokenCancelled, runtime);
            StepCaller(phase, sawTokenAtSample, c);
        }

        // Liveness: once the drain has fired, every caller must have settled. A
        // caller still Parked (or otherwise non-terminal) was lost-wakeup'd and
        // will hang the silo on shutdown.
        for (var i = 0; i < _callerCount; i++)
        {
            Specification.Assert(
                phase[i] == CallerPhase.Terminal,
                $"caller {i} never reached a terminal state after the writer drained (phase={phase[i]}): "
                + "the drain failed to release a parked admission caller - a shutdown-liveness wedge");
        }
    }

    /// <summary>Advances one admission caller by a single step.</summary>
    private void StepCaller(CallerPhase[] phase, bool[] sawTokenAtSample, int caller)
    {
        switch (phase[caller])
        {
            case CallerPhase.NotStarted:
                // Drive the real production pre-admission gate.
                if (WalAdmissionGateCore.IsDispatchRefused(_isDraining))
                {
                    // Refused with a typed shutdown fault before parking: terminal
                    // and live.
                    phase[caller] = CallerPhase.Terminal;
                }
                else
                {
                    phase[caller] = CallerPhase.PassedGate;
                }

                break;

            case CallerPhase.PassedGate:
                if (_mode == WalCommitLogWriterDrainMode.ObserveDrainTokenInWait)
                {
                    // Park with the token in the wait set: if it is already
                    // cancelled the wait completes at once, otherwise the caller
                    // parks and the token will wake it.
                    phase[caller] = _drainTokenCancelled ? CallerPhase.Terminal : CallerPhase.Parked;
                }
                else
                {
                    // Guard: sample the token now, park on the sample later.
                    sawTokenAtSample[caller] = _drainTokenCancelled;
                    phase[caller] = CallerPhase.SampledToken;
                }

                break;

            case CallerPhase.SampledToken:
                // Guard: park on the stale sample. If the token was down when
                // sampled, the caller parks without it in its wait set.
                phase[caller] = sawTokenAtSample[caller] ? CallerPhase.Terminal : CallerPhase.Parked;
                break;

            case CallerPhase.Parked:
                // A caller that observed the token (safe mode) is released when the
                // drain cancels it. In the guard mode a caller only reaches Parked
                // when it never observed the token, so it is not progressable here.
                if (_mode == WalCommitLogWriterDrainMode.ObserveDrainTokenInWait && _drainTokenCancelled)
                {
                    phase[caller] = CallerPhase.Terminal;
                }

                break;
        }
    }

    /// <summary>
    /// Whether a caller can still take a step. A parked caller can progress only
    /// in the safe mode once the drain token is cancelled; a guard-mode parked
    /// caller that never observed the token is wedged and cannot.
    /// </summary>
    private bool AnyCallerCanProgress(CallerPhase[] phase, bool[] sawTokenAtSample, bool drainTokenCancelled)
    {
        for (var i = 0; i < phase.Length; i++)
        {
            if (CallerCanProgress(phase[i], sawTokenAtSample[i], drainTokenCancelled))
            {
                return true;
            }
        }

        return false;
    }

    private bool CallerCanProgress(CallerPhase phase, bool sawToken, bool drainTokenCancelled)
    {
        return phase switch
        {
            CallerPhase.NotStarted => true,
            CallerPhase.PassedGate => true,
            CallerPhase.SampledToken => true,
            CallerPhase.Parked =>
                _mode == WalCommitLogWriterDrainMode.ObserveDrainTokenInWait && drainTokenCancelled,
            _ => false,
        };
    }

    /// <summary>
    /// Picks which progressable caller advances next, driving the choice through
    /// the runtime so the harness explores every caller/drain interleaving.
    /// </summary>
    private int SelectProgressableCaller(
        CallerPhase[] phase,
        bool[] sawTokenAtSample,
        bool drainTokenCancelled,
        ICoyoteRuntime runtime)
    {
        var fallback = -1;
        for (var i = 0; i < phase.Length; i++)
        {
            if (!CallerCanProgress(phase[i], sawTokenAtSample[i], drainTokenCancelled))
            {
                continue;
            }

            if (fallback < 0)
            {
                fallback = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return fallback;
    }
}
