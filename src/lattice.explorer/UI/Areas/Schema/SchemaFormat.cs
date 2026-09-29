using System.Globalization;
using System.Text;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>The Schema area's text: counts, rules, versions, phases, times and value previews.</summary>
internal static class SchemaFormat
{
    /// <summary>The most characters a value preview shows.</summary>
    public const int PreviewCharacters = 512;

    /// <summary>A count with its noun, such as "1 tree" or "1,204 trees".</summary>
    /// <param name="count">The count.</param>
    /// <param name="singular">The noun for one.</param>
    /// <param name="plural">The noun for any other count; the singular plus "s" when <see langword="null"/>.</param>
    /// <returns>The phrase.</returns>
    public static string Count(long count, string singular, string? plural = null) =>
        count.ToString("N0", CultureInfo.InvariantCulture) + " " + (count == 1 ? singular : plural ?? singular + "s");

    /// <summary>A time as the area shows it: UTC, to the second.</summary>
    /// <param name="value">The instant.</param>
    /// <returns>The text.</returns>
    public static string Time(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " UTC";

    /// <summary>The kind of a rule, as a word.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The word.</returns>
    public static string RuleKind(LatticeSchemaRule rule) => rule.Kind switch
    {
        LatticeSchemaRuleKind.Regex => "Pattern",
        LatticeSchemaRuleKind.Structured => "Structured",
        _ => rule.EncodingKind switch
        {
            LatticeSchemaEncodingKind.Utf8 => "UTF-8",
            LatticeSchemaEncodingKind.Json => "JSON",
            LatticeSchemaEncodingKind.MaxByteLength => "Size",
            _ => "Encoding",
        },
    };

    /// <summary>What a rule requires, in a sentence fragment.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The text.</returns>
    public static string RuleDetail(LatticeSchemaRule rule)
    {
        var detail = rule.Kind switch
        {
            LatticeSchemaRuleKind.Regex => string.IsNullOrEmpty(rule.MemberPath)
                ? $"The value matches {rule.RegexPattern}"
                : $"Member {rule.MemberPath} matches {rule.RegexPattern}",
            LatticeSchemaRuleKind.Structured => "A structured predicate over the value's members. It is shown and kept here, but not edited.",
            _ => rule.EncodingKind switch
            {
                LatticeSchemaEncodingKind.Utf8 => "The value is well-formed UTF-8",
                LatticeSchemaEncodingKind.Json => "The value is one JSON document",
                LatticeSchemaEncodingKind.MaxByteLength => $"The value is at most {Count(rule.MaxByteLength ?? 0, "byte")}",
                _ => "An encoding requirement",
            },
        };

        return string.IsNullOrWhiteSpace(rule.Description) ? detail : $"{detail} - {rule.Description}";
    }

    /// <summary>A version config in one line, such as "family 7 at version 3, strict ingest".</summary>
    /// <param name="config">The config.</param>
    /// <returns>The text.</returns>
    public static string Version(LatticeSchemaVersionConfig config) =>
        $"family {config.SchemaId} at version {config.TargetVersion}" + (config.StrictIngest ? ", strict ingest" : string.Empty);

    /// <summary>A policy in one line, such as "3 rules, strict ingest".</summary>
    /// <param name="policy">The policy.</param>
    /// <returns>The text.</returns>
    public static string Policy(LatticeSchemaPolicy policy) =>
        Count(policy.Rules.Count, "rule") + (policy.StrictIngest ? ", strict ingest" : string.Empty);

    /// <summary>A remediation phase as a word.</summary>
    /// <param name="phase">The phase.</param>
    /// <returns>The word.</returns>
    public static string Phase(LatticeSchemaRemediationPhase phase) => phase switch
    {
        LatticeSchemaRemediationPhase.Idle => "Idle",
        LatticeSchemaRemediationPhase.DryRun => "Checking every value",
        LatticeSchemaRemediationPhase.Build => "Building the remediated copy",
        LatticeSchemaRemediationPhase.Cutover => "Cutting over",
        LatticeSchemaRemediationPhase.Completed => "Completed",
        LatticeSchemaRemediationPhase.Aborted => "Aborted",
        _ => phase.ToString(),
    };

    /// <summary>A dead letter's ingest source as words.</summary>
    /// <param name="source">The source.</param>
    /// <returns>The words.</returns>
    public static string Source(LatticeSchemaDeadLetterSource source) => source switch
    {
        LatticeSchemaDeadLetterSource.Replication => "Replication",
        LatticeSchemaDeadLetterSource.Restore => "Restore",
        LatticeSchemaDeadLetterSource.LocalRejected => "Local write",
        _ => source.ToString(),
    };

    /// <summary>
    /// A value's leading bytes as text to show: UTF-8 when they decode, otherwise
    /// hexadecimal. Always rendered as text, never as markup.
    /// </summary>
    /// <param name="bytes">The bytes, or <see langword="null"/> for none.</param>
    /// <returns>The text; empty for none.</returns>
    public static string Preview(byte[]? bytes)
    {
        if (bytes is not { Length: > 0 })
        {
            return string.Empty;
        }

        string text;
        try
        {
            text = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            text = Convert.ToHexString(bytes);
        }

        return text.Length > PreviewCharacters ? text[..PreviewCharacters] + "..." : text;
    }

    /// <summary>A byte count, such as "1,024 bytes".</summary>
    /// <param name="bytes">The count.</param>
    /// <returns>The text.</returns>
    public static string Size(long bytes) => Count(bytes, "byte");
}
