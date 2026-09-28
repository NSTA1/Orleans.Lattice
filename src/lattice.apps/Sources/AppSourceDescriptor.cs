namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// The identity and shape of one named app source: its stable key, a display name, its kind and its
/// capabilities. The key is recorded as <see cref="AppProvenance.Source"/> at install, so it must never
/// change once apps have been installed from the source.
/// </summary>
public sealed record AppSourceDescriptor
{
    private const AppSourceCapabilities KnownCapabilities =
        AppSourceCapabilities.Enumerate
        | AppSourceCapabilities.Search
        | AppSourceCapabilities.MultipleVersions
        | AppSourceCapabilities.RequiresAcquisition;

    /// <summary>Creates a descriptor.</summary>
    /// <param name="key">The stable source key; must match <c>^[a-z][a-z0-9-]{1,30}$</c>.</param>
    /// <param name="displayName">The human-readable name; must not be empty or whitespace.</param>
    /// <param name="kind">Whether the offering is static or dynamic.</param>
    /// <param name="capabilities">The optional capabilities the source supports.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> or <paramref name="displayName"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="key"/> is not a valid source key, <paramref name="displayName"/> is empty or whitespace,
    /// or <paramref name="capabilities"/> carries an unknown flag.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a defined value.</exception>
    public AppSourceDescriptor(string key, string displayName, AppSourceKind kind, AppSourceCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(displayName);
        if (!IsValidKey(key))
            throw new ArgumentException("A source key must match ^[a-z][a-z0-9-]{1,30}$.", nameof(key));
        if (string.IsNullOrWhiteSpace(displayName))
            throw new ArgumentException("A source display name must not be empty.", nameof(displayName));
        if (kind is not (AppSourceKind.Static or AppSourceKind.Dynamic))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown source kind.");
        if ((capabilities & ~KnownCapabilities) != 0)
            throw new ArgumentException("The capabilities carry an unknown flag.", nameof(capabilities));

        Key = key;
        DisplayName = displayName;
        Kind = kind;
        Capabilities = capabilities;
    }

    /// <summary>The stable source key, recorded in install provenance.</summary>
    public string Key { get; }

    /// <summary>The human-readable name. It is display text only and is never interpreted.</summary>
    public string DisplayName { get; }

    /// <summary>Whether the offering is static or dynamic.</summary>
    public AppSourceKind Kind { get; }

    /// <summary>The optional capabilities the source supports.</summary>
    public AppSourceCapabilities Capabilities { get; }

    /// <summary>Whether the source supports every flag in <paramref name="capability"/>.</summary>
    /// <param name="capability">The capability flag or flags to test.</param>
    public bool Supports(AppSourceCapabilities capability) => (Capabilities & capability) == capability;

    /// <summary>Whether <paramref name="key"/> is a valid source key (<c>^[a-z][a-z0-9-]{1,30}$</c>).</summary>
    /// <param name="key">The candidate key; null is invalid.</param>
    public static bool IsValidKey(string? key) => AppSlug.TryParse(key, out _);
}
