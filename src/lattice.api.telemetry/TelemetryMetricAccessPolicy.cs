using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Decides whether a backend metric name may be read, given the configured
/// <see cref="LatticeTelemetryOptions.MetricAccess"/> posture and
/// <see cref="LatticeTelemetryOptions.AllowedMetrics"/> allow-list. The
/// telemetry tools consult it to filter listed metric names, gate a named
/// metadata lookup, and reject a query that references a metric outside the
/// allow-list.
/// </summary>
/// <remarks>
/// <para>
/// In <see cref="LatticeTelemetryMetricAccessMode.ReadAll"/> every name is
/// admitted. In <see cref="LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed"/>
/// a name is admitted only when it exactly matches a non-pattern allow-list entry
/// or matches a <c>*</c>-wildcard pattern entry. A pattern entry translates the
/// only supported wildcard <c>*</c> to a regular-expression <c>.*</c>, escapes the
/// remaining literal characters, and anchors the whole name; matching is
/// whole-name (anchored) and ordinal.
/// </para>
/// <para>
/// The exact names are held in an ordinal <see cref="HashSet{T}"/> and each
/// wildcard pattern is compiled to a <see cref="Regex"/> <b>once</b> in the
/// constructor, so a per-name admission check performs at most one set lookup and
/// a walk over the precompiled patterns and never recompiles a pattern.
/// </para>
/// <para>
/// The names tested here are caller-supplied (they arrive as a named metadata
/// lookup, or are lifted out of a submitted query), so each pattern is matched with
/// the non-backtracking engine and anchored to the very end of the input. That keeps
/// every match linear in the name's length and keeps a whole-name match exactly
/// whole-name; see the remarks on the private pattern compiler for why each of those
/// two properties is load-bearing.
/// </para>
/// </remarks>
public sealed class TelemetryMetricAccessPolicy
{
    private static readonly Regex[] NoPatterns = [];

    private readonly bool _readAll;
    private readonly HashSet<string> _exactNames;
    private readonly Regex[] _patterns;

    /// <summary>
    /// Builds the policy from the telemetry <paramref name="options"/>, splitting
    /// the allow-list into exact names and precompiled wildcard patterns.
    /// </summary>
    /// <param name="options">The telemetry options carrying the access posture and allow-list.</param>
    public TelemetryMetricAccessPolicy(LatticeTelemetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _readAll = options.MetricAccess == LatticeTelemetryMetricAccessMode.ReadAll;
        if (_readAll)
        {
            _exactNames = new HashSet<string>(StringComparer.Ordinal);
            _patterns = NoPatterns;
            return;
        }

        var exact = new HashSet<string>(StringComparer.Ordinal);
        List<Regex>? patterns = null;
        foreach (var entry in options.AllowedMetrics)
        {
            if (string.IsNullOrEmpty(entry))
            {
                continue;
            }

            if (entry.Contains('*', StringComparison.Ordinal))
            {
                (patterns ??= []).Add(Compile(entry));
            }
            else
            {
                exact.Add(entry);
            }
        }

        _exactNames = exact;
        _patterns = patterns is null ? NoPatterns : [.. patterns];
    }

    /// <summary>
    /// Whether the policy admits every metric (the
    /// <see cref="LatticeTelemetryMetricAccessMode.ReadAll"/> posture).
    /// </summary>
    public bool IsReadAll => _readAll;

    /// <summary>
    /// Returns whether the named metric is admitted under the configured posture.
    /// </summary>
    /// <param name="metric">The metric name to test.</param>
    /// <returns>
    /// <see langword="true"/> when the metric may be read; <see langword="false"/>
    /// when the deny-all posture excludes it.
    /// </returns>
    public bool IsAdmitted(string metric)
    {
        ArgumentNullException.ThrowIfNull(metric);
        if (_readAll)
        {
            return true;
        }

        if (_exactNames.Contains(metric))
        {
            return true;
        }

        foreach (var pattern in _patterns)
        {
            if (pattern.IsMatch(metric))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Translates one <c>*</c>-wildcard allow-list entry into an anchored, whole-name
    /// <see cref="Regex"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="RegexOptions.NonBacktracking"/> - not
    /// <see cref="RegexOptions.Compiled"/>, which it is mutually exclusive with -
    /// bounds every match to linear time in the length of the name being tested. The
    /// names tested here are caller-supplied, and a pattern with several wildcards
    /// (<c>lattice_*_*_*_total</c>) compiles to a chain of <c>.*</c> whose cost under
    /// the backtracking engine depends on optimiser heuristics rather than on any
    /// guarantee. Taking the guarantee instead keeps an allow-list check linear by
    /// construction (CWE-1333), which is the same posture the repository's other
    /// wire-facing matchers take. The grammar emitted here is only literals, <c>.*</c>
    /// and anchors, with no backreference or lookaround, so it is fully
    /// non-backtracking-compatible.
    /// </para>
    /// <para>
    /// The tail anchor is <c>\z</c> rather than <c>$</c> because <c>$</c> also matches
    /// immediately before a trailing newline, so <c>^lattice_.*$</c> would admit
    /// <c>lattice_x\n</c>. <see cref="RegexOptions.Singleline"/> is likewise not set,
    /// so <c>.</c> does not match a newline and a name cannot smuggle one through the
    /// middle of a wildcard either. A metric name never contains a newline, so
    /// refusing one costs no legitimate match and makes the admission decision the
    /// strict whole-name test the allow-list is documented to perform.
    /// </para>
    /// </remarks>
    private static Regex Compile(string pattern)
    {
        var builder = new StringBuilder(pattern.Length + 4).Append('^');
        foreach (var ch in pattern)
        {
            if (ch == '*')
            {
                builder.Append(".*");
            }
            else
            {
                builder.Append(Regex.Escape(ch.ToString()));
            }
        }

        builder.Append(@"\z");
        return new Regex(
            builder.ToString(),
            RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);
    }
}
