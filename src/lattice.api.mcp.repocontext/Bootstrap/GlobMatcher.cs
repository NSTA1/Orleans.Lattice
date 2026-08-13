using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A small, dependency-free glob matcher for repository-relative paths, used by
/// <see cref="RepoTreeWalker"/> to honour the caller's include / exclude filters.
/// Paths and patterns are matched using <c>'/'</c> as the only separator (the
/// walker normalises Windows separators before matching), case-insensitively.
/// <para>
/// The supported grammar is the familiar minimal subset:
/// <list type="bullet">
///   <item><description><c>**</c> matches any number of characters including
///   <c>'/'</c> (any directory depth).</description></item>
///   <item><description><c>*</c> matches any run of characters except
///   <c>'/'</c> (a single path segment).</description></item>
///   <item><description><c>?</c> matches a single character except
///   <c>'/'</c>.</description></item>
///   <item><description>every other character is matched literally.</description></item>
/// </list>
/// A pattern with no <c>'/'</c> and no <c>**</c> (for example <c>*.cs</c>) is
/// treated as a basename match at any depth, matching the common developer
/// expectation.
/// </para>
/// </summary>
internal sealed class GlobMatcher
{
    private readonly Regex _regex;

    private GlobMatcher(Regex regex) => _regex = regex;

    /// <summary>
    /// Compiles <paramref name="pattern"/> into a matcher.
    /// </summary>
    /// <param name="pattern">The glob pattern. Must not be <see langword="null"/>.</param>
    internal static GlobMatcher Compile(string pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        return new GlobMatcher(new Regex(Translate(pattern), RegexOptions.CultureInvariant | RegexOptions.IgnoreCase));
    }

    /// <summary>Reports whether <paramref name="path"/> matches this glob.</summary>
    /// <param name="path">The repository-relative, <c>'/'</c>-separated path.</param>
    internal bool IsMatch(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return _regex.IsMatch(path);
    }

    private static string Translate(string pattern)
    {
        // A bare basename pattern (no separator, no recursive wildcard) matches
        // the file name at any directory depth, so "*.cs" excludes every C# file.
        var anchored = pattern;
        if (!pattern.Contains('/') && !pattern.Contains("**", StringComparison.Ordinal))
        {
            anchored = "**/" + pattern;
        }

        var builder = new StringBuilder(anchored.Length * 2 + 4);
        builder.Append('^');
        for (var i = 0; i < anchored.Length; i++)
        {
            var c = anchored[i];
            switch (c)
            {
                case '*':
                    if (i + 1 < anchored.Length && anchored[i + 1] == '*')
                    {
                        // "**" or "**/" - match across segment boundaries.
                        i++;
                        if (i + 1 < anchored.Length && anchored[i + 1] == '/')
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
                default:
                    builder.Append(Regex.Escape(c.ToString()));
                    break;
            }
        }

        builder.Append('$');
        return builder.ToString();
    }
}
