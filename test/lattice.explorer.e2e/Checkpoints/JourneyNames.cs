namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>The naming rule for journey, step and substep ids, which become file names.</summary>
internal static class JourneyNames
{
    /// <summary>Returns <paramref name="value"/> when it is lower-case kebab-case, and throws otherwise.</summary>
    /// <param name="value">The id.</param>
    /// <param name="parameter">The parameter name, for the exception.</param>
    public static string RequireSlug(string value, string parameter)
    {
        ArgumentNullException.ThrowIfNull(value, parameter);
        if (value.Length == 0 || value.Length > 60 || value[0] == '-' || value[^1] == '-' || value.Contains("--", StringComparison.Ordinal)
            || value.Any(c => !(c is >= 'a' and <= 'z' or >= '0' and <= '9' or '-')))
        {
            throw new ArgumentException($"'{value}' is not a lower-case kebab-case id of at most 60 characters.", parameter);
        }

        return value;
    }
}
