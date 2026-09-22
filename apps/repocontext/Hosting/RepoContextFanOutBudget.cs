using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Bounds how long a batch write's per-shard fan-out may run on this deployment
/// before it is refused with a <c>LatticeSaturatedException</c>. The library
/// ships this unbounded and is right to - a finite default would be a contract
/// break for every existing caller (#3386) - but this host is not a library
/// consumer with an unknown workload, it is one known deployment whose fan-out
/// collapse was measured, so it arms the bound explicitly.
/// </summary>
/// <remarks>
/// <para>
/// The defect this bounds: a scatter-gather fan-out that awaits every branch
/// pays the <em>slowest</em> branch, so its duration tracks the branch
/// p(1 - 1/N) quantile rather than the median. Issue #3348 measured that
/// distribution splitting rather than shifting - the per-branch median improved
/// 7.7x while the 99th percentile degraded 11x, and the fan-out followed the
/// tail. Left unbounded, the only limit on a batch write is the slowest branch,
/// which is itself unbounded.
/// </para>
/// <para>
/// Why this host arms it when the library does not. The unbounded default exists
/// so that no conforming caller regresses on upgrade, which is the correct
/// default for a library whose callers are unknown. It is the wrong setting
/// <em>here</em>: this deployment's ingest path issues wide batch writes against
/// trees that are demonstrably saturated, and an unbounded fan-out converts that
/// saturation into an indefinitely-held call rather than a refusal the caller can
/// see, retry, or shed. An indefinitely-held call is the wedge - a refusal is a
/// signal.
/// </para>
/// <para>
/// Why the refusal is safe, and why it is not data loss. A batch write is not
/// atomic across shards, so this refusal rolls nothing back: branches that
/// already committed stay committed, and branches still in flight are left
/// running rather than cancelled. The durable outcome is byte-for-byte the one an
/// unbounded wait would have produced. The budget changes <em>when the caller
/// learns</em>, never <em>what is written</em>. A caller needing all-or-nothing
/// semantics across shards must use the atomic write surface, which this option
/// does not touch.
/// </para>
/// <para>
/// The hazard of over-arming is the mirror image: too small a budget refuses
/// fan-outs that a healthy cluster would have completed, turning a latency
/// problem into an availability one. The sizing rule is therefore that the budget
/// must exceed the healthy fan-out duration at the widest shard count in use, so
/// it never fires in the regime it is not meant to police.
/// </para>
/// <para>
/// Rollback is setting the variable to <c>0</c>, which restores the library
/// default of an unbounded fan-out exactly. That is a rollback to the wedge
/// rather than to data loss, which is the correct direction for this seam to
/// fail.
/// </para>
/// </remarks>
public static class RepoContextFanOutBudget
{
    /// <summary>Environment variable overriding the batch-write fan-out budget, in seconds.</summary>
    public const string FanOutBudgetKey = "LATTICE_SETMANY_FANOUT_BUDGET_SECONDS";

    /// <summary>
    /// The budget this host applies when <see cref="FanOutBudgetKey"/> is unset,
    /// in seconds. Thirty seconds is the figure the #3348 measurements support - a
    /// healthy four-silo fan-out there completed in ~6.0 s with an 8.6 s branch
    /// p99, comfortably inside the budget, while the collapsed eight-silo fan-out
    /// averaged 80.2 s and is capped by it. It also matches
    /// <c>LatticeOptions.WalAppendDispatchTimeout</c>'s own 30-second default, so a
    /// fan-out branch may not outlive the outer bound on the single WAL dispatch it
    /// is waiting for.
    /// </summary>
    public const int DefaultFanOutBudgetSeconds = 30;

    /// <summary>
    /// The largest accepted budget, in seconds. An hour is already far longer than
    /// any caller's patience, so a larger value is indistinguishable from unbounded
    /// and is better expressed as <c>0</c>, which says so explicitly.
    /// </summary>
    public const int MaxFanOutBudgetSeconds = 3600;

    /// <summary>
    /// Resolves the budget in seconds from <paramref name="configuration"/>, falling
    /// back to <see cref="DefaultFanOutBudgetSeconds"/> when
    /// <see cref="FanOutBudgetKey"/> is absent or blank. A value of <c>0</c> is
    /// accepted and means unbounded.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved budget in seconds; <c>0</c> means unbounded.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The host
    /// refuses to start rather than silently ignoring an operator's intent - a typo
    /// that quietly resolved to the default would leave the deployment enforcing a
    /// budget nobody chose.
    /// </exception>
    public static int ResolveBudgetSeconds(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[FanOutBudgetKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultFanOutBudgetSeconds;
        }

        if (!int.TryParse(raw.Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            || parsed < 0
            || parsed > MaxFanOutBudgetSeconds)
        {
            throw new InvalidOperationException(
                $"{FanOutBudgetKey} must be an integer between 0 and {MaxFanOutBudgetSeconds} (0 means unbounded); was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved budget as a <b>global</b> Lattice option on the silo.
    /// Deliberately global rather than per-tree: the fan-out is a property of the
    /// batch-write call shape, not of any one tree, and a caller batching across
    /// trees would otherwise be policed by whichever tree it happened to name.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextFanOutBudget(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var seconds = ResolveBudgetSeconds(configuration);
        silo.ConfigureLattice(options => options.SetManyFanOutBudget =
            seconds <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(seconds));
        return silo;
    }
}
