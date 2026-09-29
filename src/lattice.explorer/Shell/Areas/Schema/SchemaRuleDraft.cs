using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The policy editor's rule builder: the kind and fields of the one rule being
/// written, validated before it is added. Mutable, owned by one editor.
/// </summary>
internal sealed class SchemaRuleDraft
{
    private static readonly TimeSpan PatternCheckTimeout = TimeSpan.FromSeconds(1);

    /// <summary>The kind of rule being written.</summary>
    public SchemaRuleDraftKind Kind { get; set; } = SchemaRuleDraftKind.Utf8;

    /// <summary>The maximum byte length, as typed, for <see cref="SchemaRuleDraftKind.MaxLength"/>.</summary>
    public string MaxLength { get; set; } = string.Empty;

    /// <summary>The pattern, for <see cref="SchemaRuleDraftKind.Pattern"/>.</summary>
    public string Pattern { get; set; } = string.Empty;

    /// <summary>The member the pattern applies to, or empty for the whole value.</summary>
    public string MemberPath { get; set; } = string.Empty;

    /// <summary>An optional plain description kept with the rule.</summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>Whether the operator has typed anything since the builder was last reset.</summary>
    public bool IsDirty =>
        Kind != SchemaRuleDraftKind.Utf8
        || MaxLength.Length > 0
        || Pattern.Length > 0
        || MemberPath.Length > 0
        || Description.Length > 0;

    /// <summary>Builds the rule, or explains why it cannot be built.</summary>
    /// <param name="rule">The rule, when it can be built.</param>
    /// <param name="error">Why it cannot, as a plain sentence.</param>
    /// <returns><see langword="true"/> when the rule was built.</returns>
    public bool TryBuild(out LatticeSchemaRule rule, out string? error)
    {
        rule = default;
        error = null;
        var description = string.IsNullOrWhiteSpace(Description) ? null : Description.Trim();
        switch (Kind)
        {
            case SchemaRuleDraftKind.Utf8:
                rule = LatticeSchemaRule.Utf8(description);
                return true;

            case SchemaRuleDraftKind.Json:
                rule = LatticeSchemaRule.Json(description);
                return true;

            case SchemaRuleDraftKind.MaxLength:
                if (!int.TryParse(MaxLength.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var length))
                {
                    error = "Enter the largest size, in bytes, as a whole number.";
                    return false;
                }

                rule = LatticeSchemaRule.MaxLength(length, description);
                return true;

            default:
                if (string.IsNullOrWhiteSpace(Pattern))
                {
                    error = "Enter the pattern values must match.";
                    return false;
                }

                if (!IsValidPattern(Pattern))
                {
                    error = "That pattern is not a valid regular expression.";
                    return false;
                }

                rule = LatticeSchemaRule.Regex(Pattern, string.IsNullOrWhiteSpace(MemberPath) ? null : MemberPath.Trim(), description);
                return true;
        }
    }

    /// <summary>Clears the builder for the next rule.</summary>
    public void Reset()
    {
        Kind = SchemaRuleDraftKind.Utf8;
        MaxLength = string.Empty;
        Pattern = string.Empty;
        MemberPath = string.Empty;
        Description = string.Empty;
    }

    private static bool IsValidPattern(string pattern)
    {
        try
        {
            _ = new Regex(pattern, RegexOptions.None, PatternCheckTimeout);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }
}
