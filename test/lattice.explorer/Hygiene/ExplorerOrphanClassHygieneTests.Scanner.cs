using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The scanner behind the no-orphan-class gate: what it reads a class usage
/// out of, what it reads a class declaration out of, and how it blanks the
/// comments that would otherwise be mistaken for either.
/// </summary>
public sealed partial class ExplorerOrphanClassHygieneTests
{
    private const string ExplorerSourceRoot = "src/lattice.explorer";

    /// <summary>
    /// A class in one of the Explorer's own namespaces, as a whole token:
    /// neither preceded nor followed by a character that could continue a class
    /// name, so a CSS custom property (<c>--lx-space-4</c>) and a data
    /// attribute (<c>data-lx-breakpoint</c>) are excluded by construction.
    /// </summary>
    private static readonly Regex OwnedClass = new(
        @"(?<![-\w])lx[a-z]*-[a-zA-Z][\w-]*",
        RegexOptions.Compiled);

    /// <summary>
    /// A state modifier. Read only out of a class-valued position, where it
    /// cannot be confused with the word "is" in a sentence.
    /// </summary>
    private static readonly Regex StateClass = new(
        @"(?<![-\w])is-[a-zA-Z][\w-]*",
        RegexOptions.Compiled);

    /// <summary>A double-quoted run on a single line.</summary>
    private static readonly Regex QuotedRun = new(
        "\"[^\"\\n]*\"",
        RegexOptions.Compiled);

    /// <summary>
    /// A render-tree attribute write naming the class attribute, as in
    /// <c>builder.AddAttribute(2, "class", ...)</c>. Everything from here to
    /// the end of the statement is class-valued.
    /// </summary>
    private static readonly Regex ClassAttributeWrite = new(
        "\"class\"\\s*,",
        RegexOptions.Compiled);

    /// <summary>The selector text preceding a rule body.</summary>
    private static readonly Regex Selector = new(
        @"(?:^|\})(?<selector>[^{}]*)\{",
        RegexOptions.Compiled | RegexOptions.Singleline);

    /// <summary>A class named by a selector.</summary>
    private static readonly Regex SelectorClass = new(
        @"\.(?<name>-?[_a-zA-Z][\w-]*)",
        RegexOptions.Compiled);

    /// <summary>
    /// Every class the Explorer's markup and its code-behind name, mapped to
    /// the first file each was seen in so a failure is actionable.
    /// </summary>
    /// <param name="repoRoot">The repository root.</param>
    private static Dictionary<string, string> UsedClasses(string repoRoot)
    {
        var used = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.razor"))
        {
            var where = Relative(repoRoot, file);
            foreach (var name in ClassesInMarkup(File.ReadAllText(file)))
            {
                used.TryAdd(name, where);
            }
        }

        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.cs"))
        {
            var where = Relative(repoRoot, file);
            foreach (var name in ClassesInCode(File.ReadAllText(file)))
            {
                used.TryAdd(name, where);
            }
        }

        return used;
    }

    /// <summary>Every class an Explorer stylesheet declares.</summary>
    /// <param name="repoRoot">The repository root.</param>
    private static HashSet<string> DefinedClasses(string repoRoot)
    {
        var defined = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.css"))
        {
            foreach (var name in StylesheetClasses(File.ReadAllText(file)))
            {
                defined.Add(name);
            }
        }

        return defined;
    }

    /// <summary>
    /// The classes a source file names, in first-seen order and without
    /// repetition.
    /// </summary>
    /// <param name="source">The source to scan.</param>
    /// <param name="isCode">
    /// Whether the source is a C# file. In code every string literal is
    /// class-valued, because that is where the Explorer composes a class name
    /// it cannot write literally in markup. In Razor markup only the
    /// class-valued positions count, because markup writes element ids in the
    /// same quoted syntax and an id is not a class.
    /// </param>
    private static string[] Classes(string source, bool isCode)
    {
        var text = WithoutComments(source);
        var names = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        void Add(string value, bool withState)
        {
            foreach (Match match in OwnedClass.Matches(value))
            {
                if (seen.Add(match.Value))
                {
                    names.Add(match.Value);
                }
            }

            if (!withState)
            {
                return;
            }

            foreach (Match match in StateClass.Matches(value))
            {
                if (seen.Add(match.Value))
                {
                    names.Add(match.Value);
                }
            }
        }

        foreach (var value in ClassAttributeValues(text))
        {
            Add(value, withState: true);
        }

        foreach (var statement in ClassAttributeWrites(text))
        {
            foreach (Match run in QuotedRun.Matches(statement))
            {
                Add(run.Value, withState: true);
            }
        }

        if (!isCode)
        {
            return names.ToArray();
        }

        foreach (Match run in QuotedRun.Matches(text))
        {
            Add(run.Value, withState: false);
        }

        return names.ToArray();
    }

    /// <summary>The classes a Razor markup file names.</summary>
    /// <param name="source">The Razor source to scan.</param>
    private static string[] ClassesInMarkup(string source) => Classes(source, isCode: false);

    /// <summary>The classes a C# file names.</summary>
    /// <param name="source">The C# source to scan.</param>
    private static string[] ClassesInCode(string source) => Classes(source, isCode: true);

    /// <summary>
    /// The remainder of each statement that writes the class attribute through
    /// the render tree, so a class name a code block composes is reached
    /// without reading every other literal in the file as a class.
    /// </summary>
    /// <param name="text">The comment-blanked source to scan.</param>
    private static IEnumerable<string> ClassAttributeWrites(string text)
    {
        foreach (Match marker in ClassAttributeWrite.Matches(text))
        {
            var start = marker.Index + marker.Length;
            var end = text.IndexOf(';', start);
            yield return end < 0 ? text[start..] : text[start..end];
        }
    }

    /// <summary>The classes a stylesheet's selectors declare, in source order.</summary>
    /// <param name="css">The stylesheet text to scan.</param>
    private static string[] StylesheetClasses(string css)
    {
        var text = WithoutComments(css);
        var names = new List<string>();

        foreach (Match rule in Selector.Matches(text))
        {
            foreach (Match match in SelectorClass.Matches(rule.Groups["selector"].Value))
            {
                names.Add(match.Groups["name"].Value);
            }
        }

        return names.ToArray();
    }

    /// <summary>
    /// The value of every literal <c>class</c> (or <c>Class</c>) attribute in
    /// the text.
    /// </summary>
    /// <remarks>
    /// The value is delimited by the quote that closes it at parenthesis depth
    /// zero, not by the first quote encountered, because a Razor expression in
    /// the value carries string literals of its own -
    /// <c>class="@(ok ? "is-ok" : "is-off")"</c> is one attribute, not three.
    /// </remarks>
    /// <param name="text">The comment-blanked source to scan.</param>
    private static IEnumerable<string> ClassAttributeValues(string text)
    {
        var i = 0;
        while (i < text.Length)
        {
            if (!IsClassAttribute(text, i, out var start))
            {
                i++;
                continue;
            }

            var depth = 0;
            var end = -1;
            for (var j = start; j < text.Length && text[j] != '\n'; j++)
            {
                var c = text[j];
                if (c == '(')
                {
                    depth++;
                }
                else if (c == ')')
                {
                    depth = Math.Max(0, depth - 1);
                }
                else if (c == '"' && depth == 0)
                {
                    end = j;
                    break;
                }
            }

            if (end < 0)
            {
                i = start;
                continue;
            }

            yield return text[start..end];
            i = end + 1;
        }
    }

    /// <summary>
    /// Whether a literal class attribute starts at <paramref name="index"/>,
    /// and if so where its value begins.
    /// </summary>
    /// <param name="text">The source being scanned.</param>
    /// <param name="index">The candidate start of the attribute name.</param>
    /// <param name="valueStart">The index just past the opening quote.</param>
    private static bool IsClassAttribute(string text, int index, out int valueStart)
    {
        const string Name = "class";
        valueStart = 0;

        if (index + Name.Length > text.Length)
        {
            return false;
        }

        if (index > 0 && IsNameCharacter(text[index - 1]))
        {
            return false;
        }

        if (string.Compare(text, index, Name, 0, Name.Length, StringComparison.OrdinalIgnoreCase) != 0)
        {
            return false;
        }

        var k = index + Name.Length;
        if (k < text.Length && IsNameCharacter(text[k]))
        {
            return false;
        }

        while (k < text.Length && char.IsWhiteSpace(text[k]))
        {
            k++;
        }

        if (k >= text.Length || text[k] != '=')
        {
            return false;
        }

        k++;
        while (k < text.Length && char.IsWhiteSpace(text[k]))
        {
            k++;
        }

        if (k >= text.Length || text[k] != '"')
        {
            return false;
        }

        valueStart = k + 1;
        return true;
    }

    private static bool IsNameCharacter(char c) => char.IsLetterOrDigit(c) || c is '-' or '_';

    /// <summary>
    /// Blanks every Razor, HTML, C# and CSS comment while preserving the text's
    /// line structure, so prose naming a class it does not use is never read as
    /// a usage or a declaration.
    /// </summary>
    /// <param name="source">The source text to blank comments in.</param>
    private static string WithoutComments(string source)
    {
        var text = source.Replace("\r\n", "\n");
        text = Blank(text, @"@\*.*?\*@");
        text = Blank(text, @"<!--.*?-->");
        text = Blank(text, @"/\*.*?\*/");
        text = Blank(text, @"//[^\n]*");
        return text;
    }

    private static string Blank(string text, string pattern) =>
        Regex.Replace(
            text,
            pattern,
            match => new string(match.Value.Select(c => c == '\n' ? '\n' : ' ').ToArray()),
            RegexOptions.Singleline);

    private static IEnumerable<string> EnumerateExplorerFiles(string repoRoot, string pattern) =>
        HygieneRepository.EnumerateFiles(
            Path.Combine(repoRoot, ExplorerSourceRoot.Replace('/', Path.DirectorySeparatorChar)),
            pattern);

    /// <summary>
    /// Renders a scanned path relative to the repository root, with forward
    /// slashes, so a violation message is copy-pasteable on any platform.
    /// </summary>
    /// <param name="repoRoot">The repository root.</param>
    /// <param name="file">The absolute path of the scanned file.</param>
    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
