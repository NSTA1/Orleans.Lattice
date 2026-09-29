using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Regular expressions as the cluster runs them: linear-time
/// (<see cref="RegexOptions.NonBacktracking"/>), culture-invariant and
/// case-sensitive. The builder compiles every pattern the same way, so a pattern
/// it accepts is one setting the policy accepts, and a live test agrees with
/// enforcement.
/// </summary>
internal static class SchemaPatterns
{
    /// <summary>The longest pattern the builder accepts.</summary>
    public const int MaximumPatternLength = 2000;

    /// <summary>The longest text the live tester checks.</summary>
    public const int MaximumTestLength = 4000;

    /// <summary>The options the cluster compiles a policy pattern with.</summary>
    public const RegexOptions Options = RegexOptions.NonBacktracking | RegexOptions.CultureInvariant;

    /// <summary>Compiles a pattern known to be valid.</summary>
    /// <param name="pattern">The pattern.</param>
    /// <returns>The regex.</returns>
    public static Regex Compile(string pattern) => new(pattern, Options);

    /// <summary>Compiles <paramref name="pattern"/>, or explains why the cluster would refuse it.</summary>
    /// <param name="pattern">The pattern, as typed.</param>
    /// <param name="regex">The regex, when it compiles.</param>
    /// <param name="error">Why it does not, as a sentence.</param>
    /// <returns><see langword="true"/> when it compiles.</returns>
    public static bool TryCompile(string? pattern, out Regex? regex, out string? error)
    {
        regex = null;
        error = null;
        if (string.IsNullOrEmpty(pattern))
        {
            error = "Enter the pattern values must match.";
            return false;
        }

        if (pattern.Length > MaximumPatternLength)
        {
            error = $"A pattern may be at most {MaximumPatternLength:N0} characters.";
            return false;
        }

        try
        {
            regex = Compile(pattern);
            return true;
        }
        catch (Exception exception) when (exception is ArgumentException or NotSupportedException)
        {
            error = "The cluster cannot run this pattern: " + exception.Message
                + " Patterns run in linear time, so back-references and look-arounds are not available.";
            return false;
        }
    }

    /// <summary>The pattern equivalent to a text-match card, for showing and for taking over as a regex.</summary>
    /// <param name="match">Where the text must appear.</param>
    /// <param name="text">The text.</param>
    /// <returns>The regular expression.</returns>
    public static string ForMatch(SchemaTextMatch match, string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var escaped = Regex.Escape(text);
        return match switch
        {
            SchemaTextMatch.StartsWith => "^" + escaped,
            SchemaTextMatch.EndsWith => escaped + "$",
            _ => escaped,
        };
    }
}
