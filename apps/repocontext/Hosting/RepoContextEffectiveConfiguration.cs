using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Renders the configuration this host actually resolved into log-ready lines, marking
/// which settings were overridden, so the deployment's effective values are recoverable
/// from its own output instead of having to be inferred from whichever files happen to be
/// readable.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2294).</b> The container's real settings arrive from an
/// untracked <c>docker-compose.override.yml</c>, excluded through <c>.git/info/exclude</c>,
/// which lives in the <b>common</b> git directory and is therefore shared by every worktree
/// of one clone and invisible to every other clone and to code review. The tracked compose
/// file consequently states values the container does not run - it says a five second
/// reconcile interval while the process reports sixty - and nothing recorded the
/// divergence. The defect is not that a local override layer exists; that is a legitimate
/// and conventional pattern. The defect is that the divergence left no trace, so the
/// effective configuration was unreviewable.
/// </para>
/// <para>
/// <b>Why the evidence shape is resolved-against-default.</b> #2294's own proof was
/// "tracked says 5 / 120, the container reports 60 / 3600" - a resolved value read against
/// an expected one. Marking the overridden subset answers <i>did anything set this?</i>
/// without needing to know what did, and makes the answer greppable rather than a
/// twenty-line eyeball diff.
/// </para>
/// <para>
/// <b>Why no default is written down here.</b> A table of expected defaults in this file
/// would be a fresh unchecked claim about configuration, which is the same hazard (issue
/// #2275) that produced #2294 in the first place, merely relocated into its fix. Defaults
/// are instead <i>derived</i>, by resolving the same configuration classes a second time
/// against an empty configuration, so they cannot drift from the code that actually
/// applies them.
/// </para>
/// <para>
/// <b>Why redaction is an allowlist.</b> A denylist of credential-shaped names protects
/// the keys somebody thought of and prints in full the credential-bearing key added in six
/// months by somebody who never read this file. The failure modes are not symmetric, so
/// they are chosen on which one announces itself: printing an unclassified value leaks a
/// credential into output that is routinely pulled with <c>docker logs</c> and pasted into
/// issues, and nothing reports it; withholding a value that was safe prints
/// <c>&lt;redacted: unclassified&gt;</c>, which is visibly wrong to the first reader and
/// gets classified in the next commit. This is the repository's "fail closed" rule
/// (<c>.github/instructions/security.instructions.md</c>): ambiguity denies.
/// </para>
/// <para>
/// This type is observability only. It reads and formats; it changes no behaviour and
/// validates nothing, because a configuration this host disagrees with is still a
/// configuration the operator is entitled to run.
/// </para>
/// </remarks>
public static class RepoContextEffectiveConfiguration
{
    /// <summary>The environment-variable prefix every setting this host honours carries.</summary>
    public const string LatticePrefix = "LATTICE_";

    /// <summary>Rendered in place of a value whose key is not on <see cref="SafeToPrintKeys"/>.</summary>
    public const string UnclassifiedMarker = "<redacted: unclassified>";

    /// <summary>Rendered for a setting that has no value at all.</summary>
    public const string UnsetMarker = "<unset>";

    /// <summary>
    /// The pseudo-key under which the resolved <see cref="Environment.ProcessorCount"/> is
    /// reported. It is not an environment variable this host reads: it is the runtime fact
    /// that <c>DOTNET_PROCESSOR_COUNT</c> produces, stated directly so that it does not
    /// have to be inferred from a variable whose presence nothing records.
    /// </summary>
    public const string RuntimeProcessorCountKey = "Environment.ProcessorCount";

    /// <summary>
    /// The keys whose resolved values may be written to the log. This is an
    /// <b>allowlist</b>: a key absent from it is redacted, including a key that does not
    /// exist yet. Add a key here only after deciding its value is not credential-bearing.
    /// </summary>
    public static readonly IReadOnlySet<string> SafeToPrintKeys =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            RepoContextHostConfiguration.DurabilityKey,
            RepoContextHostConfiguration.WalProviderKey,
            RepoContextHostConfiguration.GrainStorageKey,
            RepoContextHostConfiguration.RemindersKey,
            RepoContextHostConfiguration.ClusteringKey,
            RepoContextHostConfiguration.DataRootKey,
            RepoContextHostConfiguration.WalDirKey,
            RepoContextHostConfiguration.SqlitePathKey,
            RepoContextHostConfiguration.AzureWalTableKey,
            RepoContextHostConfiguration.EmbeddingEndpointKey,
            RepoContextHostConfiguration.EmbeddingModelKey,
            RepoContextHostConfiguration.EmbeddingDimensionKey,
            RepoContextHostConfiguration.McpPortKey,
            RepoContextHostConfiguration.ClusterIdKey,
            RepoContextHostConfiguration.ServiceIdKey,
            RepoContextHostConfiguration.WorkspaceRootKey,
            RepoContextPinBucketing.PinBucketsKey,
            RepoContextReplayConcurrency.MaxConcurrentReplaysKey,
            RepoContextClaimLeases.MaxLockLeaseSecondsKey,
            RepoContextShutdownBudget.StopGracePeriodKey,

            // Not a LATTICE_ key, and deliberately reported anyway: it is the value that
            // sized the oversubscribed WAL replay gate in issue #2279, and a report scoped
            // to this project's own prefix would have missed the most damaging setting in
            // the deployment.
            RuntimeProcessorCountKey,
        };

    /// <summary>
    /// Determines whether a key's resolved value may be printed.
    /// </summary>
    /// <param name="name">The setting name.</param>
    /// <returns><see langword="true"/> when the value is classified safe to log.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public static bool IsSafeToPrint(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return SafeToPrintKeys.Contains(name);
    }

    /// <summary>
    /// Renders one value, withholding it unless its key is explicitly classified safe.
    /// </summary>
    /// <param name="name">The setting name.</param>
    /// <param name="value">The value to render, which may be null or empty.</param>
    /// <returns>The value, or a marker standing in for it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public static string RenderValue(string name, string? value)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (!IsSafeToPrint(name))
        {
            // Deliberately checked before emptiness: whether an unclassified setting was
            // supplied at all is reported by DescribeSetting's overridden marker, which
            // needs no access to the value itself.
            return UnclassifiedMarker;
        }

        return string.IsNullOrEmpty(value) ? UnsetMarker : value;
    }

    /// <summary>
    /// Renders one setting as <c>NAME = value</c>, appending an <c>[OVERRIDDEN]</c> marker
    /// and the default it departed from when the resolved value differs from the one this
    /// host reaches with nothing supplied.
    /// </summary>
    /// <param name="name">The setting name.</param>
    /// <param name="resolved">The value this host actually resolved.</param>
    /// <param name="default">The value this host resolves when nothing is supplied.</param>
    /// <returns>The rendered line.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public static string DescribeSetting(string name, string? resolved, string? @default)
    {
        ArgumentNullException.ThrowIfNull(name);

        var line = string.Create(
            CultureInfo.InvariantCulture,
            $"{name} = {RenderValue(name, resolved)}");

        return string.Equals(resolved, @default, StringComparison.Ordinal)
            ? line
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{line} [OVERRIDDEN, default {RenderValue(name, @default)}]");
    }

    /// <summary>
    /// Finds <c>LATTICE_</c> variables that were supplied to the process but that this host
    /// does not read, which is the failure mode issue #2279 actually suffered: a variable
    /// named in a compose file, carried by the container, and bound by nothing, with no
    /// error and no signal distinguishing "applied" from "ignored".
    /// </summary>
    /// <param name="environment">The process environment snapshot.</param>
    /// <param name="knownKeys">Every key this host resolves.</param>
    /// <returns>The rendered lines, empty when every supplied variable is read.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static IReadOnlyList<string> DescribeUnreadVariables(
        IEnumerable<KeyValuePair<string, string?>> environment,
        IEnumerable<string> knownKeys)
    {
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(knownKeys);

        var known = new HashSet<string>(knownKeys, StringComparer.OrdinalIgnoreCase);
        var lines = new List<string>();

        foreach (var (name, _) in environment)
        {
            if (name is null
                || !name.StartsWith(LatticePrefix, StringComparison.OrdinalIgnoreCase)
                || known.Contains(name))
            {
                continue;
            }

            lines.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"{name} = {UnclassifiedMarker} [SUPPLIED BUT NOT READ BY THIS HOST]"));
        }

        lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    /// <summary>
    /// Reads the process environment into the shape <see cref="DescribeUnreadVariables"/>
    /// accepts.
    /// </summary>
    /// <returns>The current process environment as name/value pairs.</returns>
    public static IReadOnlyList<KeyValuePair<string, string?>> ReadProcessEnvironment()
    {
        var snapshot = new List<KeyValuePair<string, string?>>();
        foreach (System.Collections.DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string name)
            {
                snapshot.Add(new KeyValuePair<string, string?>(name, entry.Value as string));
            }
        }

        return snapshot;
    }
}
