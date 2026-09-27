using System.Text.RegularExpressions;

namespace Orleans.Lattice.Apps;

/// <summary>A Semantic Version 2.0 identity, preserving prerelease and build metadata.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppVersion), Immutable]
public readonly record struct AppVersion
{
    internal const string Pattern = @"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?";
    private static readonly Regex Grammar = new(@"\A" + Pattern + @"\z", RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    private AppVersion(string value) => Value = value;

    /// <summary>The exact version text, or null for the uninitialised value.</summary>
    [Id(0)] public string Value { get; private init; }

    /// <summary>Parses a version; throws for null or invalid programmer-supplied input.</summary>
    public static AppVersion Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return TryParse(value, out var version) ? version : throw new FormatException("Invalid app semantic version.");
    }

    /// <summary>Parses untrusted version text without throwing.</summary>
    public static bool TryParse(string? value, out AppVersion version)
    {
        version = default;
        if (value is null || !Grammar.IsMatch(value))
            return false;
        version = new(value);
        return true;
    }

    /// <summary>Returns the version text, or an empty string for the uninitialised value.</summary>
    public override string ToString() => Value ?? string.Empty;
}
