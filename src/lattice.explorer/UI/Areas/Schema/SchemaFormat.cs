using System.Buffers;
using System.Globalization;
using System.Text.Unicode;
using Orleans.Lattice.Explorer.UI.Design.Components;
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
            LatticeSchemaRuleKind.Structured => StructuredRuleDetail(rule),
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

    private static string StructuredRuleDetail(LatticeSchemaRule rule)
    {
        var card = SchemaCardDecompiler.Decompile(rule);
        return card.Kind == SchemaCardKind.Custom
            ? "A structured predicate over the value's members. It is shown and kept here, but not edited."
            : SchemaCardText.Plain(card);
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
        LatticeSchemaRemediationPhase.Cancelled => "Cancelled",
        _ => phase.ToString(),
    };

    /// <summary>A tracked schema operation phase as words.</summary>
    /// <param name="phase">The operation phase.</param>
    /// <returns>The words.</returns>
    public static string OperationPhase(string phase) => phase switch
    {
        SchemaOperationPhases.Advance => "Advancing the target version",
        SchemaOperationPhases.DryRun => "Checking every value",
        SchemaOperationPhases.Build => "Building the remediated copy",
        SchemaOperationPhases.Cutover => "Cutting over",
        _ => phase,
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
    /// <param name="truncated">
    /// Whether <paramref name="bytes"/> are a byte-bounded prefix of a longer value, so they
    /// may end part-way through a character that the cut split.
    /// </param>
    /// <returns>The text; empty for none.</returns>
    public static string Preview(byte[]? bytes, bool truncated = false)
    {
        if (bytes is not { Length: > 0 })
        {
            return string.Empty;
        }

        var text = TryDecode(bytes, truncated, out var decoded) ? decoded : Convert.ToHexString(bytes);
        return text.Length > PreviewCharacters ? string.Concat(LtTextCut.Prefix(text, PreviewCharacters), "...") : text;
    }

    private static bool TryDecode(byte[] bytes, bool truncated, out string text)
    {
        // A truncated preview is cut at a byte budget, not a character boundary, so it can
        // end part-way through a character. Decoding it as a non-final block drops only that
        // incomplete tail; bytes that are not UTF-8 anywhere else still read as hexadecimal.
        var chars = new char[bytes.Length];
        var status = Utf8.ToUtf16(bytes, chars, out _, out var written, replaceInvalidSequences: false, isFinalBlock: !truncated);
        if (status is not (OperationStatus.Done or OperationStatus.NeedMoreData) || written == 0)
        {
            text = string.Empty;
            return false;
        }

        text = new string(chars, 0, written);
        return true;
    }

    /// <summary>A byte count, such as "1,024 bytes".</summary>
    /// <param name="bytes">The count.</param>
    /// <returns>The text.</returns>
    public static string Size(long bytes) => Count(bytes, "byte");
}
