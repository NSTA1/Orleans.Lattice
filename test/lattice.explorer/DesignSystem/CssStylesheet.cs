using System.Globalization;
using System.IO;
using System.Text;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// A deliberately small CSS reader: it splits a stylesheet into its top-level
/// rules and resolves the length grammar the Explorer's primitives actually
/// use, so a test can compute the geometry the shipped stylesheet produces
/// rather than assert on its text.
/// </summary>
/// <remarks>
/// <para>
/// This exists because the overflow-menu clipping fault was a <em>geometry</em>
/// fault, not a text fault. A test asserting that the stylesheet contains a
/// particular declaration proves nothing about where the menu lands; a test
/// that reads the real declarations and computes the resulting box does. It is
/// browserless and deterministic - no layout engine, no clock, no network - so
/// it runs in the unit tier.
/// </para>
/// <para>
/// The supported grammar is exactly what the primitives spend: lengths in
/// <c>px</c> and <c>rem</c>, percentages of the containing block,
/// <c>var()</c> references into the token layer, and <c>calc()</c>,
/// <c>min()</c> and <c>max()</c> over those. Anything else resolves to
/// <see langword="null"/> - "not a length I can evaluate" - and the tests
/// assert on that explicitly rather than treating it as zero, so an
/// unrecognised value fails loudly instead of passing vacuously.
/// </para>
/// </remarks>
internal sealed class CssStylesheet
{
    private readonly Dictionary<string, Dictionary<string, string>> _rules;

    private CssStylesheet(Dictionary<string, Dictionary<string, string>> rules) => _rules = rules;

    /// <summary>The root font size a <c>rem</c> is resolved against.</summary>
    public const double RootFontSizePx = 16;

    /// <summary>Reads and parses the stylesheet at <paramref name="path"/>.</summary>
    /// <param name="path">The absolute path of the stylesheet.</param>
    /// <returns>The parsed stylesheet.</returns>
    public static CssStylesheet Load(string path) => Parse(File.ReadAllText(path));

    /// <summary>Parses <paramref name="text"/> as a stylesheet.</summary>
    /// <param name="text">The stylesheet text.</param>
    /// <returns>The parsed stylesheet.</returns>
    public static CssStylesheet Parse(string text)
    {
        var css = WithoutComments(text);
        var rules = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);

        var index = 0;
        while (index < css.Length)
        {
            var open = css.IndexOf('{', index);
            if (open < 0)
            {
                break;
            }

            var selector = css[index..open].Trim();
            var close = MatchingBrace(css, open);
            if (close < 0)
            {
                break;
            }

            var body = css[(open + 1)..close];

            // An at-rule (@media, @supports) wraps further rules rather than
            // declarations. Its contents are not top-level, and no geometry
            // this reader is asked about lives inside one, so it is skipped
            // whole rather than mis-parsed as declarations.
            if (!selector.StartsWith('@'))
            {
                foreach (var name in selector.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    var trimmed = name.Trim();
                    if (trimmed.Length == 0)
                    {
                        continue;
                    }

                    if (!rules.TryGetValue(trimmed, out var declarations))
                    {
                        declarations = new Dictionary<string, string>(StringComparer.Ordinal);
                        rules[trimmed] = declarations;
                    }

                    ReadDeclarations(body, declarations);
                }
            }

            index = close + 1;
        }

        return new CssStylesheet(rules);
    }

    /// <summary>
    /// The declarations of the rule whose selector is exactly
    /// <paramref name="selector"/>, or an empty map when the stylesheet
    /// declares no such rule.
    /// </summary>
    /// <param name="selector">The selector text, for example <c>.lx-tabstrip-overflow</c>.</param>
    /// <returns>The rule's declarations.</returns>
    public IReadOnlyDictionary<string, string> Rule(string selector) =>
        _rules.TryGetValue(selector, out var declarations)
            ? declarations
            : new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>Whether the stylesheet declares a rule for <paramref name="selector"/>.</summary>
    /// <param name="selector">The selector text.</param>
    /// <returns><see langword="true"/> when the rule exists.</returns>
    public bool HasRule(string selector) => _rules.ContainsKey(selector);

    /// <summary>
    /// Every custom property declared by the <c>:root</c> rule, which is the
    /// token layer a primitive's <c>var()</c> references resolve against.
    /// </summary>
    /// <returns>The custom properties, keyed by name including the leading dashes.</returns>
    public IReadOnlyDictionary<string, string> RootCustomProperties()
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in Rule(":root"))
        {
            if (pair.Key.StartsWith("--", StringComparison.Ordinal))
            {
                properties[pair.Key] = pair.Value;
            }
        }

        return properties;
    }

    /// <summary>
    /// Resolves a CSS length to pixels.
    /// </summary>
    /// <param name="value">The declaration value, for example <c>calc(100% - 8px)</c>.</param>
    /// <param name="containingBlockWidthPx">
    /// The width a percentage resolves against, in CSS pixels.
    /// </param>
    /// <param name="variables">The custom properties <c>var()</c> resolves against.</param>
    /// <returns>
    /// The length in CSS pixels, or <see langword="null"/> when the value is a
    /// keyword (<c>auto</c>, <c>none</c>) or outside the supported grammar.
    /// </returns>
    public static double? ResolveLength(
        string? value,
        double containingBlockWidthPx,
        IReadOnlyDictionary<string, string> variables)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var reader = new LengthReader(value, containingBlockWidthPx, variables);
        var result = reader.ReadExpression(depth: 0);
        return reader.AtEnd ? result : null;
    }

    private static void ReadDeclarations(string body, Dictionary<string, string> into)
    {
        var depth = 0;
        var start = 0;
        for (var i = 0; i < body.Length; i++)
        {
            var c = body[i];
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
            }
            else if (c == ';' && depth == 0)
            {
                AddDeclaration(body[start..i], into);
                start = i + 1;
            }
        }

        if (start < body.Length)
        {
            AddDeclaration(body[start..], into);
        }
    }

    private static void AddDeclaration(string declaration, Dictionary<string, string> into)
    {
        var colon = declaration.IndexOf(':');
        if (colon < 0)
        {
            return;
        }

        var name = declaration[..colon].Trim();
        var value = declaration[(colon + 1)..].Trim();
        if (name.Length == 0 || value.Length == 0)
        {
            return;
        }

        // Later wins, exactly as the cascade resolves two declarations of the
        // same property in one rule.
        into[name] = value;
    }

    private static int MatchingBrace(string css, int open)
    {
        var depth = 0;
        for (var i = open; i < css.Length; i++)
        {
            if (css[i] == '{')
            {
                depth++;
            }
            else if (css[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    private static string WithoutComments(string css)
    {
        var builder = new StringBuilder(css.Length);
        var index = 0;
        while (index < css.Length)
        {
            var start = css.IndexOf("/*", index, StringComparison.Ordinal);
            if (start < 0)
            {
                builder.Append(css, index, css.Length - index);
                break;
            }

            builder.Append(css, index, start - index);
            var end = css.IndexOf("*/", start + 2, StringComparison.Ordinal);
            if (end < 0)
            {
                break;
            }

            builder.Append(' ');
            index = end + 2;
        }

        return builder.ToString();
    }

    /// <summary>
    /// A recursive-descent reader over the length grammar: sums and
    /// differences, products and quotients, parentheses, and the
    /// <c>var()</c> / <c>calc()</c> / <c>min()</c> / <c>max()</c> functions.
    /// </summary>
    private sealed class LengthReader(
        string text,
        double containingBlockWidthPx,
        IReadOnlyDictionary<string, string> variables)
    {
        private const int MaxDepth = 16;

        private int _position;

        public bool AtEnd
        {
            get
            {
                SkipWhitespace();
                return _position >= text.Length;
            }
        }

        public double? ReadExpression(int depth)
        {
            if (depth > MaxDepth)
            {
                return null;
            }

            var value = ReadTerm(depth);
            if (value is null)
            {
                return null;
            }

            while (true)
            {
                SkipWhitespace();
                if (_position >= text.Length)
                {
                    return value;
                }

                var op = text[_position];
                if (op is not ('+' or '-'))
                {
                    return value;
                }

                // A sign is only an operator when it is surrounded by
                // whitespace, which is what CSS calc() requires; otherwise it
                // belongs to the number that follows.
                if (_position == 0 || !char.IsWhiteSpace(text[_position - 1]))
                {
                    return value;
                }

                _position++;
                var right = ReadTerm(depth);
                if (right is null)
                {
                    return null;
                }

                value = op == '+' ? value + right : value - right;
            }
        }

        private double? ReadTerm(int depth)
        {
            var value = ReadFactor(depth);
            if (value is null)
            {
                return null;
            }

            while (true)
            {
                SkipWhitespace();
                if (_position >= text.Length)
                {
                    return value;
                }

                var op = text[_position];
                if (op is not ('*' or '/'))
                {
                    return value;
                }

                _position++;
                var right = ReadFactor(depth);
                if (right is null)
                {
                    return null;
                }

                if (op == '/' && right == 0)
                {
                    return null;
                }

                value = op == '*' ? value * right : value / right;
            }
        }

        private double? ReadFactor(int depth)
        {
            SkipWhitespace();
            if (_position >= text.Length)
            {
                return null;
            }

            var c = text[_position];
            if (c == '(')
            {
                _position++;
                var inner = ReadExpression(depth + 1);
                SkipWhitespace();
                if (_position >= text.Length || text[_position] != ')')
                {
                    return null;
                }

                _position++;
                return inner;
            }

            if (char.IsLetter(c))
            {
                return ReadFunction(depth);
            }

            return ReadNumber();
        }

        private double? ReadFunction(int depth)
        {
            var start = _position;
            while (_position < text.Length && (char.IsLetter(text[_position]) || text[_position] == '-'))
            {
                _position++;
            }

            var name = text[start.._position];
            SkipWhitespace();
            if (_position >= text.Length || text[_position] != '(')
            {
                // A bare keyword: `auto`, `none`, `static`. Not a length.
                return null;
            }

            _position++;

            if (string.Equals(name, "var", StringComparison.OrdinalIgnoreCase))
            {
                return ReadVariable(depth);
            }

            var arguments = new List<double>();
            while (true)
            {
                var argument = ReadExpression(depth + 1);
                if (argument is null)
                {
                    return null;
                }

                arguments.Add(argument.Value);
                SkipWhitespace();
                if (_position >= text.Length)
                {
                    return null;
                }

                if (text[_position] == ',')
                {
                    _position++;
                    continue;
                }

                if (text[_position] == ')')
                {
                    _position++;
                    break;
                }

                return null;
            }

            return name.ToLowerInvariant() switch
            {
                "calc" when arguments.Count == 1 => arguments[0],
                "min" when arguments.Count > 0 => arguments.Min(),
                "max" when arguments.Count > 0 => arguments.Max(),
                _ => null,
            };
        }

        private double? ReadVariable(int depth)
        {
            var start = _position;
            while (_position < text.Length && text[_position] != ')' && text[_position] != ',')
            {
                _position++;
            }

            if (_position >= text.Length)
            {
                return null;
            }

            var name = text[start.._position].Trim();

            // A fallback (`var(--x, 4px)`) is only reached when the property is
            // undefined; the token layer defines every property the primitives
            // reference, so an undefined one is a defect the test must see.
            while (_position < text.Length && text[_position] != ')')
            {
                _position++;
            }

            if (_position >= text.Length)
            {
                return null;
            }

            _position++;

            if (!variables.TryGetValue(name, out var value))
            {
                return null;
            }

            var reader = new LengthReader(value, containingBlockWidthPx, variables);
            var resolved = reader.ReadExpression(depth + 1);
            return reader.AtEnd ? resolved : null;
        }

        private double? ReadNumber()
        {
            var start = _position;
            if (_position < text.Length && (text[_position] == '-' || text[_position] == '+'))
            {
                _position++;
            }

            while (_position < text.Length && (char.IsDigit(text[_position]) || text[_position] == '.'))
            {
                _position++;
            }

            if (_position == start)
            {
                return null;
            }

            if (!double.TryParse(
                    text[start.._position],
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var number))
            {
                return null;
            }

            var unitStart = _position;
            while (_position < text.Length && (char.IsLetter(text[_position]) || text[_position] == '%'))
            {
                _position++;
            }

            var unit = text[unitStart.._position];
            return unit switch
            {
                "" => number,
                "px" => number,
                "rem" => number * RootFontSizePx,
                "em" => number * RootFontSizePx,
                "%" => number / 100.0 * containingBlockWidthPx,
                _ => null,
            };
        }

        private void SkipWhitespace()
        {
            while (_position < text.Length && char.IsWhiteSpace(text[_position]))
            {
                _position++;
            }
        }
    }
}
