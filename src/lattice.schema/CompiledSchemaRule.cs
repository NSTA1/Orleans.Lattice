using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The compiled, ready-to-evaluate form of a single <see cref="LatticeSchemaRule"/>.
/// A regex rule's <see cref="Regex"/> is compiled once (at policy-set / cache-load
/// time) so per-write evaluation never recompiles the pattern. Produced and
/// evaluated by <see cref="CompiledSchemaPolicy"/>.
/// </summary>
internal readonly struct CompiledSchemaRule
{
    private CompiledSchemaRule(
        LatticeSchemaRuleKind kind,
        LatticePredicateNode predicate,
        Regex? regex,
        string[]? memberSegments,
        LatticeSchemaEncodingKind encodingKind,
        int maxByteLength,
        string reason)
    {
        Kind = kind;
        _predicate = predicate;
        _regex = regex;
        _memberSegments = memberSegments;
        _encodingKind = encodingKind;
        _maxByteLength = maxByteLength;
        _reason = reason;
    }

    private readonly LatticePredicateNode _predicate;
    private readonly Regex? _regex;
    private readonly string[]? _memberSegments;
    private readonly LatticeSchemaEncodingKind _encodingKind;
    private readonly int _maxByteLength;
    private readonly string _reason;

    /// <summary>The kind of rule this compiled form evaluates.</summary>
    public LatticeSchemaRuleKind Kind { get; }

    /// <summary>The compiled regular expression for a <see cref="LatticeSchemaRuleKind.Regex"/> rule.</summary>
    public Regex? Regex => _regex;

    /// <summary>
    /// Compiles <paramref name="rule"/> into an evaluable form.
    /// </summary>
    /// <param name="rule">The source rule.</param>
    /// <returns>The compiled rule.</returns>
    /// <exception cref="ArgumentException">
    /// A <see cref="LatticeSchemaRuleKind.Regex"/> rule carries a pattern that
    /// cannot be compiled with <c>RegexOptions.NonBacktracking</c> (uncompilable
    /// or non-linear), or a rule is structurally incomplete for its kind.
    /// </exception>
    public static CompiledSchemaRule Compile(LatticeSchemaRule rule)
    {
        var reason = ResolveReason(rule);
        switch (rule.Kind)
        {
            case LatticeSchemaRuleKind.Structured:
                if (rule.Predicate is not { } predicate)
                {
                    throw new ArgumentException(
                        "A structured schema rule must carry a predicate.", nameof(rule));
                }

                return new CompiledSchemaRule(
                    rule.Kind, predicate, regex: null, memberSegments: null,
                    default, maxByteLength: 0, reason);

            case LatticeSchemaRuleKind.Regex:
                if (string.IsNullOrEmpty(rule.RegexPattern))
                {
                    throw new ArgumentException(
                        "A regex schema rule must carry a non-empty pattern.", nameof(rule));
                }

                var regex = CompileRegex(rule.RegexPattern);
                var segments = rule.MemberPath is null ? null : rule.MemberPath.Split('.');
                return new CompiledSchemaRule(
                    rule.Kind, default, regex, segments,
                    default, maxByteLength: 0, reason);

            case LatticeSchemaRuleKind.Encoding:
                if (rule.EncodingKind == LatticeSchemaEncodingKind.MaxByteLength && rule.MaxByteLength is not { } max)
                {
                    throw new ArgumentException(
                        "A max-byte-length encoding rule must carry a MaxByteLength.", nameof(rule));
                }

                return new CompiledSchemaRule(
                    rule.Kind, default, regex: null, memberSegments: null,
                    rule.EncodingKind, rule.MaxByteLength ?? 0, reason);

            default:
                throw new ArgumentException($"Unknown schema rule kind '{rule.Kind}'.", nameof(rule));
        }
    }

    /// <summary>
    /// Evaluates the rule against <paramref name="value"/>. Returns <c>null</c>
    /// when the value satisfies the rule; otherwise the human-readable reason it
    /// failed.
    /// </summary>
    /// <param name="value">The incoming value bytes.</param>
    /// <returns><c>null</c> when valid; otherwise the failure reason.</returns>
    public string? Validate(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Kind switch
        {
            LatticeSchemaRuleKind.Structured =>
                LatticePredicateEvaluation.Matches(value, in _predicate) ? null : _reason,
            LatticeSchemaRuleKind.Regex => ValidateRegex(value),
            LatticeSchemaRuleKind.Encoding => ValidateEncoding(value),
            _ => _reason,
        };
    }

    private string? ValidateRegex(byte[] value)
    {
        string text;
        if (_memberSegments is null)
        {
            if (!SchemaValueChecks.IsValidUtf8(value))
            {
                return _reason;
            }

            text = Encoding.UTF8.GetString(value);
        }
        else
        {
            if (SchemaValueChecks.TryProjectStringMember(value, _memberSegments) is not { } projected)
            {
                return _reason;
            }

            text = projected;
        }

        return _regex!.IsMatch(text) ? null : _reason;
    }

    private string? ValidateEncoding(byte[] value) => _encodingKind switch
    {
        LatticeSchemaEncodingKind.Utf8 => SchemaValueChecks.IsValidUtf8(value) ? null : _reason,
        LatticeSchemaEncodingKind.Json => SchemaValueChecks.IsWellFormedJson(value) ? null : _reason,
        LatticeSchemaEncodingKind.MaxByteLength => value.Length <= _maxByteLength ? null : _reason,
        _ => _reason,
    };

    private static Regex CompileRegex(string pattern)
    {
        try
        {
            // NonBacktracking guarantees linear-time matching, so no per-evaluation
            // timeout is needed; an uncompilable or non-linear pattern throws here,
            // at policy-set time, and is surfaced as a rejected policy.
            return new Regex(pattern, RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or RegexParseException)
        {
            throw new ArgumentException(
                $"The regex pattern '{pattern}' cannot be compiled with RegexOptions.NonBacktracking: {ex.Message}",
                nameof(pattern), ex);
        }
    }

    private static string ResolveReason(LatticeSchemaRule rule)
    {
        if (!string.IsNullOrEmpty(rule.Description))
        {
            return rule.Description!;
        }

        return rule.Kind switch
        {
            LatticeSchemaRuleKind.Structured => "The value did not satisfy the structured validity predicate.",
            LatticeSchemaRuleKind.Regex => rule.MemberPath is null
                ? "The value did not match the required text pattern."
                : $"The value's '{rule.MemberPath}' member did not match the required text pattern.",
            LatticeSchemaRuleKind.Encoding => rule.EncodingKind switch
            {
                LatticeSchemaEncodingKind.Utf8 => "The value is not well-formed UTF-8.",
                LatticeSchemaEncodingKind.Json => "The value is not a well-formed JSON document.",
                LatticeSchemaEncodingKind.MaxByteLength =>
                    $"The value exceeds the maximum of {rule.MaxByteLength} bytes.",
                _ => "The value failed the encoding rule.",
            },
            _ => "The value failed schema validation.",
        };
    }
}
