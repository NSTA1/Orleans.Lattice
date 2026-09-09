using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The accumulated <c>.gitignore</c> rule state in effect at one directory of a
/// repository walk, layered as an immutable chain from the repository root down to
/// the current directory. Each layer holds the compiled rules parsed from the
/// <c>.gitignore</c> file at a particular directory, anchored to that directory.
/// <para>
/// <b>Why a chain.</b> The tree walk descends one directory at a time; a directory
/// that carries its own <c>.gitignore</c> derives a child scope through
/// <see cref="Add"/> while a directory without one simply reuses its parent scope
/// reference, so the common no-<c>.gitignore</c> directory adds no allocation. A
/// path is classified by folding every applicable layer from the shallowest to the
/// deepest and taking the last match, which reproduces Git's precedence: a pattern
/// in a deeper file overrides a shallower one, and within a file the last matching
/// pattern wins (including a re-including <c>!</c> negation).
/// </para>
/// <para>
/// <b>Scope of fidelity.</b> This is a practical, dependency-free subset of the
/// <c>gitignore</c>(5) grammar covering the forms real repositories use: comments,
/// blank lines, <c>!</c> negation, a leading <c>/</c> or an interior <c>/</c> to
/// anchor a pattern to its <c>.gitignore</c> directory, a trailing <c>/</c> for a
/// directory-only match, the <c>*</c>, <c>?</c>, and <c>**</c> wildcards, a
/// <c>[...]</c> character class (with ranges and <c>!</c>/<c>^</c> negation, as in
/// the ubiquitous <c>[Bb]in/</c> and <c>[Oo]bj/</c>), and a leading <c>\</c> escape
/// of <c>#</c> or <c>!</c>. It does not read <c>.git/info/exclude</c> or the user's
/// global excludes.
/// </para>
/// <para>
/// <b>Hierarchy.</b> Each rule matches the entry it names and nothing deeper; a path
/// beneath an ignored directory is excluded because <see cref="IsIgnored"/>
/// classifies every ancestor directory first and the shallowest ignored ancestor
/// decides. That reproduces the Git rule that a path under an excluded directory
/// cannot be re-included, and - the reason it matters here - it stops a
/// <c>!</c> negation naming a directory from re-including that directory's whole
/// subtree, which would silently readmit the build output a later-losing
/// <c>[Bb]in/</c> rule had excluded.
/// </para>
/// </summary>
internal sealed class GitignoreScope
{
    private readonly GitignoreScope? _parent;
    private readonly string _baseDir;
    private readonly IReadOnlyList<GitignoreRule> _rules;

    private GitignoreScope(GitignoreScope? parent, string baseDir, IReadOnlyList<GitignoreRule> rules)
    {
        _parent = parent;
        _baseDir = baseDir;
        _rules = rules;
    }

    /// <summary>The empty scope: no rules, so nothing is ignored.</summary>
    public static GitignoreScope Empty { get; } = new(null, string.Empty, []);

    /// <summary>
    /// Derives a child scope that layers the rules parsed from
    /// <paramref name="content"/> - the text of a <c>.gitignore</c> located at the
    /// repository-relative directory <paramref name="baseDir"/> (the empty string
    /// for the repository root) - on top of this scope. Returns the same scope
    /// unchanged when the file contributes no effective rule, so a directory whose
    /// <c>.gitignore</c> is empty or all comments adds no layer.
    /// </summary>
    /// <param name="baseDir">The repository-relative, <c>'/'</c>-separated directory
    /// the <c>.gitignore</c> sits in (empty for the root). Must not be <see langword="null"/>.</param>
    /// <param name="content">The raw <c>.gitignore</c> text. Must not be <see langword="null"/>.</param>
    public GitignoreScope Add(string baseDir, string content)
    {
        ArgumentNullException.ThrowIfNull(baseDir);
        ArgumentNullException.ThrowIfNull(content);

        var rules = GitignoreRule.Parse(content);
        return rules.Count == 0 ? this : new GitignoreScope(this, baseDir, rules);
    }

    /// <summary>
    /// Reports whether <paramref name="relativePath"/> is ignored by the layered
    /// rules. The path is repository-relative and <c>'/'</c>-separated;
    /// <paramref name="isDirectory"/> selects whether directory-only patterns apply.
    /// <para>
    /// Evaluation is hierarchical, as Git's is: every ancestor directory is
    /// classified first and the shallowest ignored ancestor decides, so an entry
    /// beneath an excluded directory is excluded and cannot be re-included by a
    /// later negation. Only when no ancestor is ignored is the entry itself
    /// classified, by <see cref="IsEntryIgnored"/>.
    /// </para>
    /// </summary>
    /// <param name="relativePath">The repository-relative path to classify. Must not be <see langword="null"/>.</param>
    /// <param name="isDirectory"><see langword="true"/> when the path names a directory.</param>
    public bool IsIgnored(string relativePath, bool isDirectory)
    {
        ArgumentNullException.ThrowIfNull(relativePath);

        for (var slash = relativePath.IndexOf('/'); slash >= 0;
            slash = relativePath.IndexOf('/', slash + 1))
        {
            if (IsEntryIgnored(relativePath[..slash], isDirectory: true))
            {
                return true;
            }
        }

        return IsEntryIgnored(relativePath, isDirectory);
    }

    /// <summary>
    /// Reports whether <paramref name="relativePath"/> is ignored considering the
    /// entry itself only, on the caller's guarantee that no ancestor directory is
    /// ignored. The tree walk holds that guarantee by construction: it prunes an
    /// ignored directory rather than descending it, so every entry it classifies
    /// already sits under a chain of directories it found not ignored. Callers
    /// without that guarantee must use <see cref="IsIgnored"/>, which establishes it
    /// by classifying each ancestor first.
    /// </summary>
    /// <param name="relativePath">The repository-relative path to classify. Must not be <see langword="null"/>.</param>
    /// <param name="isDirectory"><see langword="true"/> when the path names a directory.</param>
    public bool IsEntryIgnored(string relativePath, bool isDirectory)
    {
        ArgumentNullException.ThrowIfNull(relativePath);
        bool? decision = null;
        Evaluate(this, relativePath, isDirectory, ref decision);
        return decision ?? false;
    }

    private static void Evaluate(GitignoreScope? scope, string relativePath, bool isDirectory, ref bool? decision)
    {
        if (scope is null || scope._rules.Count == 0 && scope._parent is null)
        {
            return;
        }

        // Fold the shallowest layer first so a deeper file's later rule wins.
        Evaluate(scope._parent, relativePath, isDirectory, ref decision);

        if (!TryMakeRelative(scope._baseDir, relativePath, out var scoped))
        {
            return;
        }

        foreach (var rule in scope._rules)
        {
            if (rule.Matches(scoped, isDirectory))
            {
                decision = !rule.Negated;
            }
        }
    }

    /// <summary>
    /// Reduces a repository-relative path to a path relative to a layer's base
    /// directory, so the layer's rules (anchored at that directory) match against
    /// the correct segment. Returns <see langword="false"/> when the path is not
    /// under the base directory, in which case the layer does not apply.
    /// </summary>
    private static bool TryMakeRelative(string baseDir, string relativePath, out string scoped)
    {
        if (baseDir.Length == 0)
        {
            scoped = relativePath;
            return true;
        }

        if (relativePath.Length > baseDir.Length
            && relativePath[baseDir.Length] == '/'
            && relativePath.StartsWith(baseDir, StringComparison.Ordinal))
        {
            scoped = relativePath[(baseDir.Length + 1)..];
            return true;
        }

        scoped = string.Empty;
        return false;
    }

    /// <summary>
    /// One compiled <c>.gitignore</c> pattern: its negation flag, its directory-only
    /// flag, and a regular expression matched against a path already made relative
    /// to the owning <c>.gitignore</c> directory.
    /// </summary>
    private sealed class GitignoreRule
    {
        private readonly Regex _regex;
        private readonly bool _directoryOnly;

        private GitignoreRule(Regex regex, bool negated, bool directoryOnly)
        {
            _regex = regex;
            Negated = negated;
            _directoryOnly = directoryOnly;
        }

        /// <summary>Whether the rule re-includes (a leading <c>!</c>) rather than excludes.</summary>
        public bool Negated { get; }

        /// <summary>
        /// Reports whether a base-relative path matches this rule. The match is on
        /// the entry itself only: a path nested beneath the pattern is covered by
        /// the caller classifying each ancestor directory in turn (see
        /// <see cref="GitignoreScope.IsIgnored"/>), which is what keeps a negation
        /// from re-including a whole subtree the way Git never does.
        /// </summary>
        public bool Matches(string scopedPath, bool isDirectory) =>
            _regex.IsMatch(scopedPath) && (!_directoryOnly || isDirectory);

        /// <summary>Parses <c>.gitignore</c> text into ordered rules, skipping blank and comment lines.</summary>
        public static IReadOnlyList<GitignoreRule> Parse(string content)
        {
            var rules = new List<GitignoreRule>();
            foreach (var rawLine in content.Split('\n'))
            {
                var rule = ParseLine(rawLine);
                if (rule is not null)
                {
                    rules.Add(rule);
                }
            }

            return rules;
        }

        private static GitignoreRule? ParseLine(string rawLine)
        {
            var line = StripTrailingSpaces(rawLine.TrimEnd('\r'));
            if (line.Length == 0)
            {
                return null;
            }

            // A leading '#' is a comment unless escaped as '\#'.
            if (line[0] == '#')
            {
                return null;
            }

            var negated = false;
            if (line[0] == '!')
            {
                negated = true;
                line = line[1..];
            }
            else if (line.StartsWith("\\#", StringComparison.Ordinal)
                || line.StartsWith("\\!", StringComparison.Ordinal))
            {
                line = line[1..];
            }

            if (line.Length == 0)
            {
                return null;
            }

            var directoryOnly = line.EndsWith('/');
            if (directoryOnly)
            {
                line = line[..^1];
            }

            if (line.Length == 0)
            {
                return null;
            }

            // A slash anywhere (leading or interior, after the trailing slash was
            // removed) anchors the pattern to the .gitignore directory; otherwise it
            // matches at any depth below it.
            var anchored = line.Contains('/');
            if (line[0] == '/')
            {
                line = line[1..];
                anchored = true;
            }

            if (line.Length == 0)
            {
                return null;
            }

            // The non-backtracking engine, not the compiled one. A .gitignore rule
            // is evaluated against every path discovered under its directory, so
            // this is the hottest matcher in the walk - but the pattern is authored
            // by whoever wrote the indexed repository, not by an authorized
            // operator, and Translate emits "(?:.*/)?" per "**/" segment. Under the
            // backtracking engine a committed line such as "**/**/**/.../x" is
            // exponential in its segment count on a non-matching path, so a single
            // checked-in file could pin the indexer thread indefinitely (CWE-1333).
            // NonBacktracking evaluates the same language in time linear in the
            // path, which bounds that cost for every rule and every path, and it is
            // mutually exclusive with Compiled so the JIT-compiled engine is given
            // up deliberately: a linear-time guarantee on attacker-authored input
            // outranks a constant-factor win on trusted input. The emitted grammar
            // (literals, character classes, '.', '*', '?', anchors, non-capturing
            // groups) carries no backreference, lookaround, or atomic group, so it
            // is compatible by construction.
            const RegexOptions options = RegexOptions.CultureInvariant | RegexOptions.NonBacktracking;
            return new GitignoreRule(
                new Regex(Translate(line, anchored) + "$", options),
                negated,
                directoryOnly);
        }

        private static string StripTrailingSpaces(string line)
        {
            var end = line.Length;
            while (end > 0 && line[end - 1] == ' ')
            {
                // A space escaped by a backslash is significant and kept.
                if (end >= 2 && line[end - 2] == '\\')
                {
                    break;
                }

                end--;
            }

            return end == line.Length ? line : line[..end];
        }

        /// <summary>
        /// Translates a <c>.gitignore</c> pattern into the body of a regular
        /// expression (anchored at <c>^</c> but with no terminator), so the caller
        /// can append the <c>$</c> terminator that matches the entry itself.
        /// </summary>
        private static string Translate(string pattern, bool anchored)
        {
            var builder = new StringBuilder(pattern.Length * 2 + 8);
            builder.Append('^');

            // A non-anchored pattern matches at any directory depth, i.e. against
            // the basename or any deeper segment sequence.
            if (!anchored)
            {
                builder.Append("(?:.*/)?");
            }

            for (var i = 0; i < pattern.Length; i++)
            {
                var c = pattern[i];
                switch (c)
                {
                    case '*':
                        if (i + 1 < pattern.Length && pattern[i + 1] == '*')
                        {
                            i++;
                            if (i + 1 < pattern.Length && pattern[i + 1] == '/')
                            {
                                i++;
                                builder.Append("(?:.*/)?");
                            }
                            else
                            {
                                builder.Append(".*");
                            }
                        }
                        else
                        {
                            builder.Append("[^/]*");
                        }

                        break;
                    case '?':
                        builder.Append("[^/]");
                        break;
                    case '/':
                        builder.Append('/');
                        break;
                    case '[':
                        i = AppendCharacterClass(pattern, i, builder);
                        break;
                    default:
                        builder.Append(Regex.Escape(c.ToString()));
                        break;
                }
            }

            return builder.ToString();
        }

        /// <summary>
        /// Translates a <c>.gitignore</c> bracket expression that opens at
        /// <paramref name="open"/> into a regular-expression character class,
        /// appends it to <paramref name="builder"/>, and returns the index of the
        /// closing <c>]</c> so the caller's loop resumes after it. A <c>!</c> or
        /// <c>^</c> immediately after the <c>[</c> negates the class, a <c>-</c>
        /// between two members is a range, and a <c>]</c> as the first member is a
        /// literal. An unterminated <c>[</c> is treated as a literal bracket. Like
        /// the rest of the grammar the class never matches the path separator, so a
        /// negated class excludes <c>/</c> too.
        /// </summary>
        private static int AppendCharacterClass(string pattern, int open, StringBuilder builder)
        {
            var close = FindClosingBracket(pattern, open);
            if (close < 0)
            {
                // No terminator: a lone '[' is a literal character.
                builder.Append("\\[");
                return open;
            }

            var inner = open + 1;
            builder.Append('[');
            var negated = inner < close && (pattern[inner] == '!' || pattern[inner] == '^');
            if (negated)
            {
                // Exclude the separator so a negated class cannot cross a directory
                // boundary, matching the pathname semantics of the rest of the grammar.
                builder.Append("^/");
                inner++;
            }

            for (var k = inner; k < close; k++)
            {
                var cc = pattern[k];
                switch (cc)
                {
                    // '-' keeps its range meaning; the regex class understands it the
                    // same way gitignore does. The metacharacters that are special
                    // inside a regex class are escaped so a member stays literal.
                    case '-':
                        builder.Append('-');
                        break;
                    case ']':
                    case '\\':
                    case '^':
                    case '[':
                        builder.Append('\\').Append(cc);
                        break;
                    default:
                        builder.Append(cc);
                        break;
                }
            }

            builder.Append(']');
            return close;
        }

        /// <summary>
        /// Finds the index of the <c>]</c> that closes the bracket expression opened
        /// at <paramref name="open"/>, honouring a leading negation marker and a
        /// <c>]</c> that appears as the first member (which is a literal, not a
        /// terminator). Returns <c>-1</c> when the expression is unterminated.
        /// </summary>
        private static int FindClosingBracket(string pattern, int open)
        {
            var k = open + 1;
            if (k < pattern.Length && (pattern[k] == '!' || pattern[k] == '^'))
            {
                k++;
            }

            // A ']' immediately here is a literal first member, not the terminator.
            if (k < pattern.Length && pattern[k] == ']')
            {
                k++;
            }

            for (; k < pattern.Length; k++)
            {
                if (pattern[k] == ']')
                {
                    return k;
                }
            }

            return -1;
        }
    }
}
