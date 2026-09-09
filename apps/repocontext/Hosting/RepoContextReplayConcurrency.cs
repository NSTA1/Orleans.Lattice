using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Lets an operator pin the per-silo ceiling on concurrent activation-time leaf
/// WAL replays (<see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/>)
/// from the environment, so a deployment whose CPU quota and
/// <see cref="Environment.ProcessorCount"/> disagree can state the ceiling it
/// actually wants instead of inheriting one derived from a number the runtime
/// was told to report.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this class has to exist at all (issue #2279).</b> The library already
/// honours an explicit ceiling: a positive
/// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/> takes
/// precedence over the <see cref="Environment.ProcessorCount"/> default, so the
/// documented remedy for an oversubscribed gate is "pin the option". That remedy
/// was not reachable from a container, because <b>this host has no generic
/// configuration-to-options binding</b>. Every <c>LATTICE_*</c> variable it
/// honours is plumbed by hand, one class per option, and no class read this one.
/// Adding the variable to a compose file therefore did nothing whatsoever, and
/// did it silently: nothing parses an unrecognised environment variable, so
/// there was no error, no warning, and no signal distinguishing "pinned" from
/// "ignored". This class is the missing plumbing, and it is deliberately the
/// smallest thing that makes the already-documented remedy true.
/// </para>
/// <para>
/// <b>Why the default is deferral rather than a number.</b> The gate exists to
/// bound a reactivation storm against the CPU the process can actually obtain,
/// so the only defensible ceiling is one derived from the deployment's real
/// quota. A host default cannot know that quota: reading it would mean parsing
/// cgroup limits to second-guess <c>DOTNET_PROCESSOR_COUNT</c>, which is a
/// documented and supported knob that every other .NET subsystem obeys, and
/// which the process would then hold two conflicting beliefs about. So the
/// default here is <see cref="DefaultMaxConcurrentReplays"/> (<c>0</c>), which
/// defers to the library and changes nothing for any deployment that does not
/// opt in. Unlike <see cref="RepoContextPinBucketing"/>, whose host default
/// deliberately overrides the library, this class adds a capability rather than
/// an opinion.
/// </para>
/// <para>
/// <b>Set the value where the quota is set.</b> The ceiling and the CPU quota
/// are two halves of one statement, and they are only checkable against each
/// other when they are declared together. Pinning a literal in a file that does
/// not itself constrain CPU would create a second assertion nothing verifies -
/// the same hazard (issue #2275) that produced #2279 in the first place, merely
/// relocated from a source comment into a compose file. So the value belongs
/// beside the <c>cpus</c> / <c>NanoCpus</c> limit for the deployment, and the
/// resolved figure is observable at run time in the gate's own startup log line.
/// </para>
/// </remarks>
public static class RepoContextReplayConcurrency
{
    /// <summary>
    /// Environment variable pinning the per-silo concurrent leaf WAL replay ceiling.
    /// </summary>
    public const string MaxConcurrentReplaysKey = "LATTICE_WAL_MAX_CONCURRENT_REPLAYS";

    /// <summary>
    /// The value applied when <see cref="MaxConcurrentReplaysKey"/> is unset
    /// (<c>0</c>): defer to the library, which resolves the ceiling to
    /// <see cref="Environment.ProcessorCount"/>. Deferring keeps this wiring
    /// inert for every deployment that has not opted in.
    /// </summary>
    public const int DefaultMaxConcurrentReplays = LatticeOptions.DefaultWalMaterialiserMaxConcurrentReplays;

    /// <summary>
    /// The largest accepted ceiling. Each permit admits one whole-readable-window
    /// WAL replay, which is CPU bound, so a value far above any plausible host's
    /// core count does not bound a reactivation storm at all - it merely restates
    /// "unbounded" in a way that reads as configured.
    /// </summary>
    public const int MaxConcurrentReplaysCeiling = 256;

    /// <summary>
    /// Resolves the replay ceiling from <paramref name="configuration"/>, falling
    /// back to <see cref="DefaultMaxConcurrentReplays"/> when
    /// <see cref="MaxConcurrentReplaysKey"/> is absent or blank.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>
    /// The resolved ceiling, between <c>0</c> (defer to the library) and
    /// <see cref="MaxConcurrentReplaysCeiling"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The
    /// host refuses to start rather than silently ignoring an operator's intent,
    /// because silently ignoring it is the exact failure this class was written
    /// to remove.
    /// </exception>
    public static int ResolveMaxConcurrentReplays(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[MaxConcurrentReplaysKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultMaxConcurrentReplays;
        }

        if (!int.TryParse(raw.Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            || parsed < 0
            || parsed > MaxConcurrentReplaysCeiling)
        {
            throw new InvalidOperationException(
                $"{MaxConcurrentReplaysKey} must be an integer between 0 and {MaxConcurrentReplaysCeiling} "
                + $"(zero defers to the library, which sizes the gate from Environment.ProcessorCount); was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved ceiling as a <b>global</b> Lattice option on the silo.
    /// Global rather than per-tree by necessity, not preference: the replay gate is
    /// a single process-wide semaphore, sized once by the first leaf activation
    /// that resolves options and never re-created. A per-tree registration would
    /// therefore make the effective ceiling depend on which tree happened to
    /// activate first, which is a race rather than a configuration.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextReplayConcurrency(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var max = ResolveMaxConcurrentReplays(configuration);
        silo.ConfigureLattice(options => options.WalMaterialiserMaxConcurrentReplays = max);
        return silo;
    }
}
