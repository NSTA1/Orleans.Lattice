using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>Validates a pending wrapper's issue reference.</summary>
public static partial class PendingIssue
{
    /// <summary>Returns <paramref name="issue"/> when it is <c>#N</c>.</summary>
    /// <param name="issue">The reference.</param>
    /// <exception cref="ArgumentException">It is not <c>#N</c>.</exception>
    public static string Validate(string issue)
    {
        if (issue is null || !Shape().IsMatch(issue))
        {
            throw new ArgumentException($"A pending wrapper names its tracking issue as '#N', not '{issue}'.", nameof(issue));
        }

        return issue;
    }

    [GeneratedRegex("^#[1-9][0-9]*$")]
    private static partial Regex Shape();
}
