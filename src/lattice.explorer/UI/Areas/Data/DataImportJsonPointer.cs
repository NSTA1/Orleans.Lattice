using System.Text.Json;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// An RFC 6901 JSON Pointer (<c>/order/id</c>, <c>/lines/0</c>) that an import resolves
/// against each NDJSON line to find its key or its value (issue #5365).
/// </summary>
internal sealed class DataImportJsonPointer
{
    private readonly string[] _tokens;

    private DataImportJsonPointer(string text, string[] tokens)
    {
        Text = text;
        _tokens = tokens;
    }

    /// <summary>The pointer as written.</summary>
    public string Text { get; }

    /// <summary>Parses <paramref name="text"/> as a pointer to a member or element (never the whole line).</summary>
    /// <param name="text">The pointer, such as <c>/id</c>.</param>
    /// <param name="pointer">The pointer, when the text is one.</param>
    /// <param name="error">What is wrong, when it is not.</param>
    /// <returns><see langword="true"/> when <paramref name="text"/> is a pointer.</returns>
    public static bool TryParse(string? text, out DataImportJsonPointer? pointer, out string? error)
    {
        pointer = null;
        if (string.IsNullOrEmpty(text) || text[0] != '/')
        {
            error = $"\"{text}\" is not a JSON Pointer: it starts with / and names a field, such as /id.";
            return false;
        }

        var raw = text[1..].Split('/');
        var tokens = new string[raw.Length];
        for (var i = 0; i < raw.Length; i++)
        {
            var token = raw[i];
            for (var j = 0; j < token.Length; j++)
            {
                if (token[j] == '~' && (j + 1 >= token.Length || token[j + 1] is not ('0' or '1')))
                {
                    error = $"\"{text}\" is not a JSON Pointer: ~ must be written ~0, and / inside a name ~1.";
                    return false;
                }
            }

            tokens[i] = token.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
        }

        pointer = new DataImportJsonPointer(text, tokens);
        error = null;
        return true;
    }

    /// <summary>Resolves the pointer against <paramref name="root"/>.</summary>
    /// <param name="root">The line's JSON value.</param>
    /// <param name="value">The value the pointer names, when it names one.</param>
    /// <returns><see langword="true"/> when the line has a value at the pointer.</returns>
    public bool TryResolve(JsonElement root, out JsonElement value)
    {
        value = root;
        foreach (var token in _tokens)
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.Object when value.TryGetProperty(token, out var member):
                    value = member;
                    break;
                case JsonValueKind.Array when IsIndex(token, out var index) && index < value.GetArrayLength():
                    value = value[index];
                    break;
                default:
                    value = default;
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The text a key segment takes from <paramref name="value"/>: a string's text, or a
    /// number's or boolean's literal. An object, an array or null names no key.
    /// </summary>
    /// <param name="value">The resolved value.</param>
    /// <returns>The text, or <see langword="null"/> when the value is not a scalar.</returns>
    public static string? ScalarText(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.GetRawText(),
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        _ => null,
    };

    private static bool IsIndex(string token, out int index)
    {
        index = 0;
        if (token.Length == 0 || (token.Length > 1 && token[0] == '0'))
        {
            return false;
        }

        foreach (var c in token)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return int.TryParse(token, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out index);
    }
}
