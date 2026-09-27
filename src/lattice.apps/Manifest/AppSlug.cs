namespace Orleans.Lattice.Apps;

/// <summary>
/// Ordinal app identity matching <c>^[a-z][a-z0-9-]{1,30}$</c>.
/// Underscores are reserved for MCP namespacing. The default value is invalid.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppSlug), Immutable]
public readonly record struct AppSlug
{
    private AppSlug(string value) => Value = value;

    /// <summary>The slug text, or null for the uninitialised value.</summary>
    [Id(0)] public string Value { get; private init; }

    /// <summary>Parses a slug; throws for null or invalid programmer-supplied input.</summary>
    public static AppSlug Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return TryParse(value, out var slug) ? slug : throw new FormatException("Invalid app slug.");
    }

    /// <summary>Parses untrusted text without throwing; failure returns the default value.</summary>
    public static bool TryParse(string? value, out AppSlug slug)
    {
        slug = default;
        if (value is null || value.Length is < 2 or > 31 || value[0] is < 'a' or > 'z')
            return false;
        foreach (var c in value)
            if (c is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-')
                return false;
        slug = new(value);
        return true;
    }

    /// <summary>Returns the slug, or an empty string for the uninitialised value.</summary>
    public override string ToString() => Value ?? string.Empty;
}
