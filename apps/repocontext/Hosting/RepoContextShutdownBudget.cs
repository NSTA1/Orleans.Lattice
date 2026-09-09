using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Derives the host's shutdown budget (<c>HostOptions.ShutdownTimeout</c>) from
/// the <b>grant the container actually gives it</b> - the deployment's
/// <c>stop_grace_period</c>, declared to the process through
/// <see cref="StopGracePeriodKey"/> - rather than fixing it as a second,
/// independent constant that nothing ties to the first.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the budget is not a free parameter (issue #2402).</b> The budget looks
/// like a number that can be raised when a drain outgrows it. It is not.
/// <c>stop_grace_period</c> is a hard ceiling imposed from outside the process:
/// Docker sends <c>SIGTERM</c> and then <c>SIGKILL</c> at the grace period
/// whatever the host is doing, and the process can neither observe that value nor
/// change it. So a budget set <b>above</b> the grant buys no drain time at all.
/// What it does instead is strictly worse than leaving it alone: it arms
/// <see cref="RepoContextDrainSignal"/>'s overrun alarm for an instant the process
/// never lives to reach, so the <c>drain ABANDONED</c> line - the one piece of
/// evidence that a drain was cut short - is never emitted. That silent teardown is
/// the defect recorded as issue #2389, and raising the budget past the grant
/// reintroduces it by way of the very change meant to prevent it.
/// </para>
/// <para>
/// <b>Why the budget is therefore derived from the grant and not from residency.</b>
/// Issue #2402 observes, correctly, that a fixed budget drains a resident
/// activation set with no observed ceiling, and proposes deriving the budget from
/// observed residency and per-write cost. That cannot be done honestly here.
/// <c>HostOptions.ShutdownTimeout</c> is captured by the generic host at
/// construction and its timeout source is created <b>before</b>
/// <c>ApplicationStopping</c> is raised, so a shutdown-time computation is already
/// too late; and a budget that tracked residency would climb straight past a grant
/// the process cannot see, silencing the alarm exactly as above. The grant is the
/// quantity that genuinely bounds the budget, so the grant is what it is derived
/// from.
/// </para>
/// <para>
/// <b>What this does not fix.</b> Drain time remains proportional to the resident
/// activation set, and nothing here bounds that set. This removes the second
/// independent guess and makes "budget exceeds grant" impossible by construction;
/// it does not address the cause, which is tracked separately.
/// </para>
/// <para>
/// <b>Set the value where the grace period is set.</b> The declared grant and the
/// real <c>stop_grace_period</c> are two halves of one statement and are only
/// checkable against each other when they are written together, so
/// <see cref="StopGracePeriodKey"/> belongs beside <c>stop_grace_period</c> in the
/// same compose service - the same reasoning that puts
/// <see cref="RepoContextReplayConcurrency.MaxConcurrentReplaysKey"/> beside the
/// <c>cpus</c> limit. Adjacency is the mitigation and it is a <b>convention, not
/// an enforcement</b>: an operator who edits one and not the other leaves the
/// process deriving a budget from a stale declaration, and the dangerous direction
/// (declared larger than actual) cannot be detected at run time, because the
/// premise of this whole class is that the real grace period is unobservable from
/// inside the container.
/// </para>
/// </remarks>
public static class RepoContextShutdownBudget
{
    /// <summary>
    /// Environment variable declaring the container's <c>stop_grace_period</c> -
    /// the interval the deployment grants between <c>SIGTERM</c> and
    /// <c>SIGKILL</c> - to the process that must fit inside it.
    /// </summary>
    /// <remarks>
    /// Accepts a whole or fractional number of seconds, with an optional <c>s</c>
    /// suffix so the value can be written identically in both halves of the pair
    /// (<c>120s</c> here and <c>stop_grace_period: 120s</c> beside it).
    /// </remarks>
    public const string StopGracePeriodKey = "LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD";

    /// <summary>
    /// The fraction of the grant the drain may consume before the host abandons it.
    /// </summary>
    /// <remarks>
    /// The remainder is reserve: the host still has to unwind the stop sequence,
    /// emit the <c>drain ABANDONED</c> line, and let the logger flush it, and all of
    /// that happens after the budget expires but before <c>SIGKILL</c>. A drain that
    /// consumed the whole grant would silence the very line that reports it was cut
    /// short.
    /// <para>
    /// <b>The value is calibrated, not measured, and that is stated rather than
    /// dressed up.</b> 0.75 is chosen so the shipped pair is reproduced exactly -
    /// the sample deployment's 120s grant yields the 90s budget the container has
    /// run with since it shipped - so this class changes no deployed behaviour and
    /// moves no number. What it buys is structural: an operator now sets one value
    /// instead of two independent ones, and a budget above the grant becomes
    /// unrepresentable rather than merely discouraged.
    /// </para>
    /// </remarks>
    public const double BudgetFractionOfGrant = 0.75;

    /// <summary>
    /// The floor on the reserve left between the budget and the grant.
    /// </summary>
    /// <remarks>
    /// <see cref="BudgetFractionOfGrant"/> alone mis-scales, because <b>the cost it
    /// reserves for is roughly constant while the reserve it produces is
    /// proportional</b>. Emitting one log line and flushing it costs about the same
    /// whatever the grant is, so a percentage reserve is merely wasteful at a large
    /// grant (a 600s grant would reserve 150s for a sub-second unwind) and
    /// <b>insufficient at a small one</b>: a 4s grant would reserve 1s, which is not
    /// reliably enough to log and flush before <c>SIGKILL</c>. That failure is
    /// silent, and it would land on whoever configured the tightest grace period -
    /// which is to say on the deployment least able to absorb it. Flooring the
    /// reserve at a constant removes the small-grant case without disturbing the
    /// large one.
    /// <para>
    /// The two rules cross over at a grant of
    /// <see cref="UnwindReserve"/> / (1 - <see cref="BudgetFractionOfGrant"/>) = 8
    /// seconds: below that the constant reserve binds, above it the fraction does.
    /// The intended operating range is tens of seconds to a few minutes, where the
    /// fraction binds and this floor never engages.
    /// </para>
    /// </remarks>
    public static readonly TimeSpan UnwindReserve = TimeSpan.FromSeconds(2);

    /// <summary>
    /// The grant assumed when <see cref="StopGracePeriodKey"/> is not declared: the
    /// value the sample compose file has always set.
    /// </summary>
    /// <remarks>
    /// Defaulting rather than refusing to start keeps this wiring inert for every
    /// deployment that has not opted in, which is what makes it safe to land: an
    /// existing container that sets no new variable derives exactly the budget it
    /// already ran with.
    /// </remarks>
    public static readonly TimeSpan DefaultStopGracePeriod = TimeSpan.FromSeconds(120);

    /// <summary>
    /// The largest accepted grant. A grace period beyond an hour does not bound a
    /// teardown in any useful sense; it restates "unbounded" in a form that reads as
    /// configured, which is the shape of the defect this class exists to remove.
    /// </summary>
    public static readonly TimeSpan MaxStopGracePeriod = TimeSpan.FromHours(1);

    /// <summary>
    /// The budget derived from <see cref="DefaultStopGracePeriod"/>: 90 seconds, the
    /// value the container has run with since it shipped.
    /// </summary>
    /// <remarks>
    /// Computed from the derivation rather than written as a literal, so the claim
    /// that the derivation reproduces the shipped pair is enforced by the code
    /// rather than asserted in a comment beside it.
    /// </remarks>
    public static readonly TimeSpan DefaultShutdownBudget = Derive(DefaultStopGracePeriod);

    /// <summary>
    /// Derives the shutdown budget from a container grant, leaving the larger of a
    /// proportional and a constant reserve.
    /// </summary>
    /// <param name="stopGracePeriod">The container's grace period between signals.</param>
    /// <returns>The budget the host may spend draining, strictly less than the grant.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The grant is not positive, or is too small to leave a positive budget once the
    /// unwind reserve is taken. A non-positive budget is refused rather than clamped
    /// to something nominal, because a budget of zero disables the graceful drain
    /// while still reading as configured.
    /// </exception>
    public static TimeSpan Derive(TimeSpan stopGracePeriod)
    {
        if (stopGracePeriod <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(stopGracePeriod),
                stopGracePeriod,
                "The container stop grace period must be positive.");
        }

        var proportional = stopGracePeriod * BudgetFractionOfGrant;
        var floored = stopGracePeriod - UnwindReserve;
        var budget = proportional < floored ? proportional : floored;

        if (budget <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(stopGracePeriod),
                stopGracePeriod,
                $"A stop grace period of {stopGracePeriod.TotalSeconds}s leaves no shutdown budget once the "
                + $"{UnwindReserve.TotalSeconds}s unwind reserve is taken, so the host could not report a "
                + $"cut-short drain before being killed. Grant more than {UnwindReserve.TotalSeconds}s.");
        }

        return budget;
    }

    /// <summary>
    /// Resolves the declared grant and the budget derived from it, reporting whether
    /// the grant was declared or defaulted so the distinction is visible in the
    /// startup log rather than inferred.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved grant, budget, and declaration state.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not a positive number of seconds within the
    /// accepted range. The host refuses to start rather than silently ignoring an
    /// operator's intent, because a grant that is quietly discarded leaves the
    /// process deriving its budget from a value nobody chose.
    /// </exception>
    public static RepoContextShutdownBudgetResolution Resolve(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[StopGracePeriodKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return new RepoContextShutdownBudgetResolution(
                DefaultStopGracePeriod,
                DefaultShutdownBudget,
                GrantWasDeclared: false);
        }

        var grant = ParseStopGracePeriod(raw);
        return new RepoContextShutdownBudgetResolution(grant, Derive(grant), GrantWasDeclared: true);
    }

    /// <summary>
    /// Parses a declared grace period: a positive number of seconds, with an optional
    /// <c>s</c> suffix.
    /// </summary>
    /// <param name="raw">The raw configured value.</param>
    /// <returns>The parsed grace period.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="raw"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The value is not an accepted duration.</exception>
    public static TimeSpan ParseStopGracePeriod(string raw)
    {
        ArgumentNullException.ThrowIfNull(raw);

        var trimmed = raw.Trim();
        if (trimmed.EndsWith('s') || trimmed.EndsWith('S'))
        {
            trimmed = trimmed[..^1].TrimEnd();
        }

        if (!double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds)
            || double.IsNaN(seconds)
            || double.IsInfinity(seconds)
            || seconds <= 0d
            || seconds > MaxStopGracePeriod.TotalSeconds)
        {
            // Compose's own duration grammar admits compound forms such as 1m30s, so
            // an operator copying a value across from stop_grace_period can legitimately
            // arrive here with one. Naming that case is the difference between being
            // told and being silently misread.
            throw new InvalidOperationException(
                $"{StopGracePeriodKey} must be a positive number of seconds, optionally suffixed with 's' "
                + $"(for example '120' or '120s'), and no greater than {MaxStopGracePeriod.TotalSeconds}s; "
                + $"was '{raw}'. Compound durations such as '1m30s' are not accepted here - state the same "
                + "interval in seconds so it reads identically to the stop_grace_period it declares.");
        }

        return TimeSpan.FromSeconds(seconds);
    }
}
