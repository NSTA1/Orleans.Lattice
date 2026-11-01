using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

/// <summary>
/// Applies <see cref="GrainStorageFencingProbeLogSuppression"/> to the host's
/// logging: wraps the filter of every <see cref="LoggerFilterRule"/> so that a
/// record the grain storage provider writes while serving the fencing probe is
/// dropped (issue #5442).
/// </summary>
/// <remarks>
/// <para>
/// Microsoft.Extensions.Logging picks exactly one rule per provider and category,
/// so a new rule for the provider's category would replace the level the host
/// configured for it. Wrapping the filter of every existing rule instead leaves
/// rule selection, every configured level and every existing filter exactly as
/// they were; the wrapper only adds the probe check in front of the original
/// filter. When no rule applies to every provider and category, one is added at
/// <see cref="LoggerFilterOptions.MinLevel"/>, which is the level the logging
/// pipeline falls back to in that case anyway, so the check also covers
/// categories no configured rule matches.
/// </para>
/// <para>
/// The wrapper runs on every enabled log call. While no probe runs it costs one
/// delegate call and one volatile read, with no allocation.
/// </para>
/// </remarks>
internal sealed class GrainStorageFencingProbeLogFilter : IPostConfigureOptions<LoggerFilterOptions>
{
    /// <inheritdoc />
    public void PostConfigure(string? name, LoggerFilterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var rules = options.Rules;
        var coversEverything = false;
        for (var i = 0; i < rules.Count; i++)
        {
            var rule = rules[i];
            coversEverything |= rule.ProviderName is null && rule.CategoryName is null;
            if (rule.Filter?.Target is Gate)
            {
                continue;
            }

            rules[i] = new LoggerFilterRule(rule.ProviderName, rule.CategoryName, rule.LogLevel, new Gate(rule.Filter).Filter);
        }

        if (!coversEverything)
        {
            rules.Add(new LoggerFilterRule(null, null, options.MinLevel, new Gate(null).Filter));
        }
    }

    /// <summary>
    /// The probe check placed in front of a rule's original filter.
    /// </summary>
    private sealed class Gate(Func<string?, string?, LogLevel, bool>? inner)
    {
        internal bool Filter(string? provider, string? category, LogLevel level)
            => !GrainStorageFencingProbeLogSuppression.ShouldSuppress(level)
                && (inner is null || inner(provider, category, level));
    }
}
