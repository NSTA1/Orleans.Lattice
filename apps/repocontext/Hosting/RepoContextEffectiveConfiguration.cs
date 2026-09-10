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
    /// States the boundary this report speaks inside, so that a setting it does not name
    /// is read as out of scope rather than as unset.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this exists (issue #2470).</b> The report enumerates settings resolved from
    /// the process environment. It never said so, and an enumeration that does not state
    /// its scope is read as exhaustive - which is precisely the trust the surrounding
    /// prose invites. An operator checking whether a value was applied, finding no line for
    /// it, and concluding it was unset would be wrong in the one direction that matters:
    /// the setting may be configured, applied, and steering the runtime, through a channel
    /// this report cannot see.
    /// </para>
    /// <para>
    /// The omission is worst for the settings nobody thought to look for, because those are
    /// the ones an operator relies on the enumeration to have covered. Absence is only
    /// evidence of absence once the boundary containing it is written down.
    /// </para>
    /// </remarks>
    public const string ScopeStatement =
        "SCOPE: this report covers settings resolved from the process environment (the "
        + "LATTICE_ variables listed above) plus Environment.ProcessorCount. It does NOT "
        + "cover LatticeOptions configured in code through ConfigureLattice - WalRetention "
        + "among them - nor any value supplied through a channel other than the process "
        + "environment. A setting absent from this report is a setting outside its scope, "
        + "not a setting proven unset.";

    /// <summary>Rendered in place of the value of a variable matched only by prefix.</summary>
    public const string PrefixMatchedMarker = "<withheld: matched by prefix only>";

    /// <summary>
    /// The keys whose resolved values may be written to the log. This is an
    /// <b>allowlist</b>: a key absent from it is redacted, including a key that does not
    /// exist yet. Add a key here only after deciding its value is not credential-bearing.
    /// </summary>
    public static readonly IReadOnlySet<string> SafeToPrintKeys = BuildSafeToPrintKeys();

    private static IReadOnlySet<string> BuildSafeToPrintKeys()
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
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

        // The repository-context package's own settings. Every one is a cadence, a count,
        // an enum, or a staging path - none is credential-bearing, so each is classified
        // deliberately rather than by inheriting the classification of its neighbours. The
        // per-repository git settings, which DO include tokens, are NOT added: they are a
        // prefix family that is never described individually, so they stay unclassified
        // and therefore withheld.
        foreach (var key in RepoContextEnvironmentVariables.All)
        {
            keys.Add(key);
        }

        return keys;
    }

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
        => DescribeUnreadVariables(environment, knownKeys, []);

    /// <summary>
    /// Finds <c>LATTICE_</c> variables that were supplied to the process but that this host
    /// does not read, ignoring any variable that belongs to one of
    /// <paramref name="knownPrefixes"/>.
    /// </summary>
    /// <remarks>
    /// The prefixes exist because some variables carry a repository id in the middle of
    /// their name, so their full names are not knowable until run time and no exact-match
    /// set can cover them. Without this, every correctly-supplied member of such a family
    /// would be reported <c>[SUPPLIED BUT NOT READ BY THIS HOST]</c> - the same false
    /// negative issue #2460 was filed about, one level down. Prefix-matched variables are
    /// not silently dropped either; see <see cref="DescribePrefixMatchedVariables"/>.
    /// </remarks>
    /// <param name="environment">The process environment snapshot.</param>
    /// <param name="knownKeys">Every exactly-named key this host resolves.</param>
    /// <param name="knownPrefixes">Every prefix under which this host resolves a family.</param>
    /// <returns>The rendered lines, empty when every supplied variable is read.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static IReadOnlyList<string> DescribeUnreadVariables(
        IEnumerable<KeyValuePair<string, string?>> environment,
        IEnumerable<string> knownKeys,
        IEnumerable<string> knownPrefixes)
    {
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(knownKeys);
        ArgumentNullException.ThrowIfNull(knownPrefixes);

        var known = new HashSet<string>(knownKeys, StringComparer.OrdinalIgnoreCase);
        var prefixes = knownPrefixes.ToArray();
        var lines = new List<string>();

        foreach (var (name, _) in environment)
        {
            if (name is null
                || !name.StartsWith(LatticePrefix, StringComparison.OrdinalIgnoreCase)
                || known.Contains(name)
                || MatchesPrefix(name, prefixes))
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
    /// Finds supplied variables that this host recognises only by prefix, and reports them
    /// as such.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reported rather than dropped, because prefix membership is a weaker claim than
    /// name-for-name recognition. This host knows the family is read; it does not know
    /// that this particular member names a setting anything binds, so a typo inside a
    /// recognised family is indistinguishable from a correct member. Counting such a
    /// variable as read would trade one unsupported claim for another, which is the defect
    /// issue #2460 records rather than a fix for it.
    /// </para>
    /// <para>
    /// Values are always withheld here. A member of the git-source family may be a
    /// personal access token, and the allowlist that would otherwise decide cannot classify
    /// a name it has never seen - so this path never consults it and never prints a value.
    /// </para>
    /// </remarks>
    /// <param name="environment">The process environment snapshot.</param>
    /// <param name="knownKeys">Every exactly-named key this host resolves.</param>
    /// <param name="knownPrefixes">Every prefix under which this host resolves a family.</param>
    /// <returns>The rendered lines, empty when nothing matched a prefix only.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static IReadOnlyList<string> DescribePrefixMatchedVariables(
        IEnumerable<KeyValuePair<string, string?>> environment,
        IEnumerable<string> knownKeys,
        IEnumerable<string> knownPrefixes)
    {
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(knownKeys);
        ArgumentNullException.ThrowIfNull(knownPrefixes);

        var known = new HashSet<string>(knownKeys, StringComparer.OrdinalIgnoreCase);
        var prefixes = knownPrefixes.ToArray();
        var lines = new List<string>();

        foreach (var (name, _) in environment)
        {
            if (name is null || known.Contains(name) || !MatchesPrefix(name, prefixes))
            {
                continue;
            }

            lines.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"{name} = {PrefixMatchedMarker} [MATCHED BY A PREFIX THIS HOST READS, NOT VERIFIED INDIVIDUALLY]"));
        }

        lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    private static bool MatchesPrefix(string name, IReadOnlyList<string> prefixes)
    {
        foreach (var prefix in prefixes)
        {
            // The name must be longer than the prefix: a variable equal to the prefix is
            // not a member of the family, and admitting it would let a bare prefix
            // masquerade as a recognised setting.
            if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                && name.Length > prefix.Length)
            {
                return true;
            }
        }

        return false;
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
