using System.Text;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// How an import builds each entry's key (issue #5365): one field reference on its own,
/// or a template mixing literal text with references in braces. For NDJSON a reference is
/// a JSON Pointer (<c>/id</c>, or <c>order/{/id}</c>); for CSV it is a column name
/// (<c>id</c>, or <c>order/{id}</c>). <c>{{</c> and <c>}}</c> write a literal brace.
/// </summary>
internal sealed class DataImportKeyTemplate
{
    private readonly (string? Literal, string? Reference)[] _segments;
    private readonly StringBuilder _builder = new();

    private DataImportKeyTemplate((string? Literal, string? Reference)[] segments, IReadOnlyList<string> references)
    {
        _segments = segments;
        References = references;
    }

    /// <summary>The field references the template reads, in order: pointers for NDJSON, column names for CSV.</summary>
    public IReadOnlyList<string> References { get; }

    /// <summary>Parses <paramref name="text"/> as a key template.</summary>
    /// <param name="text">The template, such as <c>/id</c>, <c>order/{/id}</c>, <c>id</c> or <c>order/{id}</c>.</param>
    /// <param name="pointers">Whether references are JSON Pointers (NDJSON) rather than column names (CSV).</param>
    /// <param name="template">The template, when the text is one.</param>
    /// <param name="error">What is wrong, when it is not.</param>
    /// <returns><see langword="true"/> when <paramref name="text"/> is a template that reads at least one field.</returns>
    public static bool TryParse(string? text, bool pointers, out DataImportKeyTemplate? template, out string? error)
    {
        template = null;
        error = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            error = pointers
                ? "Say where the key comes from: a JSON Pointer such as /id, or a template such as order/{/id}."
                : "Say where the key comes from: a column such as id, or a template such as order/{id}.";
            return false;
        }

        if (text.IndexOfAny(['{', '}']) < 0)
        {
            // A bare reference: the whole text names the field.
            if (pointers && !DataImportJsonPointer.TryParse(text, out _, out error))
            {
                return false;
            }

            template = new DataImportKeyTemplate([(null, text)], [text]);
            return true;
        }

        var segments = new List<(string?, string?)>();
        var references = new List<string>();
        var literal = new StringBuilder();
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '{' && i + 1 < text.Length && text[i + 1] == '{')
            {
                literal.Append('{');
                i++;
            }
            else if (c == '}' && i + 1 < text.Length && text[i + 1] == '}')
            {
                literal.Append('}');
                i++;
            }
            else if (c == '{')
            {
                var close = text.IndexOf('}', i + 1);
                if (close < 0)
                {
                    error = $"The key template has a {{ at position {i + 1} with no closing }}.";
                    return false;
                }

                var reference = text[(i + 1)..close];
                if (reference.Length == 0)
                {
                    error = "The key template has an empty {}: name a field inside the braces.";
                    return false;
                }

                if (pointers && !DataImportJsonPointer.TryParse(reference, out _, out error))
                {
                    return false;
                }

                if (literal.Length > 0)
                {
                    segments.Add((literal.ToString(), null));
                    literal.Clear();
                }

                segments.Add((null, reference));
                references.Add(reference);
                i = close;
            }
            else if (c == '}')
            {
                error = $"The key template has a }} at position {i + 1} with no opening {{; write }}}} for a literal brace.";
                return false;
            }
            else
            {
                literal.Append(c);
            }
        }

        if (literal.Length > 0)
        {
            segments.Add((literal.ToString(), null));
        }

        if (references.Count == 0)
        {
            error = "The key template reads no field, so every line would get the same key: name a field in braces.";
            return false;
        }

        template = new DataImportKeyTemplate([.. segments], references);
        return true;
    }

    /// <summary>
    /// Builds a key, reading each reference through <paramref name="lookup"/>, which
    /// returns the field's text or <see langword="null"/> with a reason it has none.
    /// </summary>
    /// <param name="lookup">Reads one field, by its index in <see cref="References"/>.</param>
    /// <param name="key">The key, when every field has text.</param>
    /// <param name="error">Why the record has no key, when a field has no text.</param>
    /// <returns><see langword="true"/> when the key was built.</returns>
    public bool TryBuild(Func<int, (string? Text, string? Error)> lookup, out string? key, out string? error)
    {
        ArgumentNullException.ThrowIfNull(lookup);
        key = null;
        error = null;
        if (_segments.Length == 1 && _segments[0].Reference is not null)
        {
            var (text, problem) = lookup(0);
            if (text is null)
            {
                error = problem;
                return false;
            }

            key = text;
            return true;
        }

        _builder.Clear();
        var reference = 0;
        foreach (var (literalText, field) in _segments)
        {
            if (field is null)
            {
                _builder.Append(literalText);
                continue;
            }

            var (text, problem) = lookup(reference++);
            if (text is null)
            {
                error = problem;
                return false;
            }

            _builder.Append(text);
        }

        key = _builder.ToString();
        return true;
    }
}
