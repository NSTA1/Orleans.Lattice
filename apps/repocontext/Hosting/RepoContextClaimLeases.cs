using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Raises the ceiling this host clamps every granted or renewed named-lock lease
/// to, so an agent holding a backlog claim across a build-and-test cycle is not
/// evicted mid-work.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the library default is wrong for this host, specifically.</b> The
/// library caps a lease at <see cref="LatticeOptions.MaxLockLeaseDurationValue"/>
/// (5 minutes) so a crashed holder cannot wedge a <i>contended</i> lock for
/// hours. That reasoning is sound for the workload the ceiling was chosen for:
/// short, contended, machine-speed critical sections, where 5 minutes is already
/// far longer than any healthy holder needs.
/// </para>
/// <para>
/// The agent-backlog claim surface is the opposite workload in every respect
/// that matters to this figure. Its critical section is a unit of human-scale
/// work - build, test, review, push - which routinely exceeds 5 minutes, and its
/// locks are essentially uncontended, because the claim exists to assign an item
/// to exactly one agent rather than to serialise a hot path. Against that
/// workload the ceiling is shorter than the shortest useful unit of work, which
/// converts lease expiry from a rare fault-recovery event into a routine one.
/// </para>
/// <para>
/// <b>The condition under which the old behaviour bites, which is the part worth
/// keeping.</b> On the first live multi-agent run of the backlog protocol, two
/// independent workers each had a claim lapse mid-build. Both re-claimed cleanly
/// and the fencing token incremented exactly as designed, so nothing was
/// corrupted - but during each lapse the item was, to any other worker computing
/// the ready set, indistinguishable from one that had never been claimed. The
/// ready set drops items held under a <i>live</i> claim, and there was no live
/// claim. Only timing prevented a second worker starting duplicate work on an
/// item already being built. Raising the ceiling does not make that race
/// impossible - the fencing token is what makes it <i>safe</i> - but it removes
/// the routine trigger, leaving expiry to mean what it was designed to mean: the
/// holder is gone.
/// </para>
/// <para>
/// <b>What is deliberately not changed.</b> The library default is untouched, so
/// no other deployment's contended locks inherit a looser bound. The
/// <see cref="LatticeOptions.DefaultLockLeaseDuration"/> (30 seconds) is also
/// untouched: a caller that omits a lease still gets a short one, because a
/// caller that did not think about the lease length is exactly the caller that
/// should not be granted a long one. Only the ceiling an explicit request may
/// reach is raised.
/// </para>
/// </remarks>
public static class RepoContextClaimLeases
{
    /// <summary>Environment variable overriding the maximum named-lock lease, in seconds.</summary>
    public const string MaxLockLeaseSecondsKey = "LATTICE_MAX_LOCK_LEASE_SECONDS";

    /// <summary>
    /// The ceiling this host applies when <see cref="MaxLockLeaseSecondsKey"/> is
    /// unset. Thirty minutes comfortably covers a full build-and-test cycle plus
    /// the margin an agent needs to renew around one, while still bounding how
    /// long a crashed holder can pin a backlog item.
    /// </summary>
    public const int DefaultMaxLockLeaseSeconds = 1800;

    /// <summary>
    /// The smallest accepted ceiling. The option's own contract requires the
    /// maximum to be at least
    /// <see cref="LatticeOptions.DefaultLockLeaseDurationValue"/> (30 seconds),
    /// since a ceiling below the default would silently clamp every
    /// defer-to-default acquisition.
    /// </summary>
    public const int MinMaxLockLeaseSeconds = 30;

    /// <summary>
    /// The largest accepted ceiling. Beyond two hours the lease stops being a
    /// liveness bound in any useful sense: a crashed holder would pin its item
    /// for longer than most agent sessions live, so the stale-claim reaper - the
    /// entire reason the lock is lease-bounded rather than flag-held - would no
    /// longer recover the item within the run that needs it.
    /// </summary>
    public const int MaxMaxLockLeaseSeconds = 7200;

    /// <summary>
    /// Resolves the lease ceiling from <paramref name="configuration"/>, falling
    /// back to <see cref="DefaultMaxLockLeaseSeconds"/> when
    /// <see cref="MaxLockLeaseSecondsKey"/> is absent or blank.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>
    /// The resolved ceiling in seconds, between <see cref="MinMaxLockLeaseSeconds"/>
    /// and <see cref="MaxMaxLockLeaseSeconds"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The
    /// host refuses to start rather than silently ignoring an operator's intent.
    /// </exception>
    public static int ResolveMaxLeaseSeconds(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[MaxLockLeaseSecondsKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultMaxLockLeaseSeconds;
        }

        if (!int.TryParse(raw.Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            || parsed < MinMaxLockLeaseSeconds
            || parsed > MaxMaxLockLeaseSeconds)
        {
            throw new InvalidOperationException(
                $"{MaxLockLeaseSecondsKey} must be an integer between {MinMaxLockLeaseSeconds} and {MaxMaxLockLeaseSeconds}; was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved ceiling as a <b>global</b> Lattice option on the silo.
    /// Deliberately global rather than per-tree, because
    /// <c>LatticeLockGrain</c> resolves its clamp from the unnamed options
    /// instance: a named lock is not owned by any one tree.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextClaimLeases(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var seconds = ResolveMaxLeaseSeconds(configuration);
        silo.ConfigureLattice(options => options.MaxLockLeaseDuration = TimeSpan.FromSeconds(seconds));
        return silo;
    }
}
