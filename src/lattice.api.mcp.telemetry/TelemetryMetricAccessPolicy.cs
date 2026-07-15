using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Decides whether a backend metric name may be read, given the configured
/// <see cref="LatticeApiMcpTelemetryOptions.MetricAccess"/> posture and
/// <see cref="LatticeApiMcpTelemetryOptions.AllowedMetrics"/> allow-list. The
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
/// </remarks>
internal sealed class TelemetryMetricAccessPolicy
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
    public TelemetryMetricAccessPolicy(LatticeApiMcpTelemetryOptions options)
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

        builder.Append('$');
        return new Regex(
            builder.ToString(),
            RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    }
}
