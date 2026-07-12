using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Low-level, allocation-conscious value checks shared by the compiled schema
/// rules: strict UTF-8 validity, JSON well-formedness, and projecting a string
/// member out of a value's JSON document for a regex rule. Kept separate from the
/// rule types so the checks are unit-testable in isolation.
/// </summary>
internal static class SchemaValueChecks
{
    // Throwing encoder: GetCharCount raises DecoderFallbackException on any
    // invalid byte sequence, so UTF-8 validity is checked without allocating a
    // decoded string on the success path.
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// Returns <c>true</c> when <paramref name="value"/> decodes as well-formed
    /// UTF-8. Allocation-free on the success path.
    /// </summary>
    public static bool IsValidUtf8(byte[] value)
    {
        try
        {
            _ = StrictUtf8.GetCharCount(value);
            return true;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="value"/> is a single well-formed
    /// JSON document. Validates via a forward-only reader without materializing a
    /// document object.
    /// </summary>
    public static bool IsWellFormedJson(byte[] value)
    {
        if (value.Length == 0)
        {
            return false;
        }

        try
        {
            var reader = new Utf8JsonReader(value, isFinalBlock: true, state: default);
            while (reader.Read())
            {
                // Advance through every token; malformed input throws.
            }

            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// Projects the string member at the dotted <paramref name="memberPath"/> out
    /// of <paramref name="value"/>'s JSON document. Property-name matching is
    /// ordinal and case-insensitive, mirroring the predicate evaluator. Returns
    /// <c>null</c> when the payload is not JSON, the path does not resolve, or the
    /// resolved member is not a JSON string.
    /// </summary>
    public static string? TryProjectStringMember(byte[] value, string memberPath)
    {
        ArgumentNullException.ThrowIfNull(memberPath);
        return TryProjectStringMember(value, memberPath.Split('.'));
    }

    /// <summary>
    /// Projects the string member addressed by the pre-split
    /// <paramref name="memberSegments"/> out of <paramref name="value"/>'s JSON
    /// document. This overload takes the path segments already split so a hot-path
    /// caller (a compiled regex rule) never re-splits per evaluation. Semantics
    /// otherwise match <see cref="TryProjectStringMember(byte[], string)"/>.
    /// </summary>
    public static string? TryProjectStringMember(byte[] value, string[] memberSegments)
    {
        if (value.Length == 0)
        {
            return null;
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(value);
        }
        catch (JsonException)
        {
            return null;
        }

        using (document)
        {
            var current = document.RootElement;
            foreach (var segment in memberSegments)
            {
                if (current.ValueKind != JsonValueKind.Object
                    || !TryGetPropertyIgnoreCase(current, segment, out current))
                {
                    return null;
                }
            }

            return current.ValueKind == JsonValueKind.String ? current.GetString() : null;
        }
    }

    private static bool TryGetPropertyIgnoreCase(JsonElement element, string name, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
