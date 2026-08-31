namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// What a refused caller must do about a denial: the grant they are missing and
/// who can issue it.
/// <para>
/// A denial that says only "this is not available for your account" states no
/// remedy, so the reader learns nothing they can act on. This carries the two
/// facts that make a denial actionable as <em>structured data</em> rather than a
/// pre-composed English sentence, so the shell owns the wording (and its
/// translation), the gate owns the facts, and neither has to parse the other.
/// </para>
/// <para>
/// It is a <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> of two string references, and a gate declares its
/// own once as a static, so carrying a remedy on every resolved decision costs
/// no allocation on the render path. <see cref="Describe"/> composes a sentence
/// only when a caller asks for one.
/// </para>
/// </summary>
public readonly record struct ExplorerAccessRemedy
{
    /// <summary>
    /// The remedy for a denial whose missing grant is not known - the fallback,
    /// and the value of <c>default</c>. Renders as no remedy at all rather than
    /// as a misleading one.
    /// </summary>
    public static ExplorerAccessRemedy None { get; }

    private ExplorerAccessRemedy(string permission, string audience)
    {
        Permission = permission;
        Audience = audience;
    }

    /// <summary>
    /// The grant the caller is missing, named the way the cluster names it - for
    /// example <c>Backup</c> or <c>Admin</c>. <see langword="null"/> when the
    /// denial names no specific grant.
    /// </summary>
    public string? Permission { get; }

    /// <summary>
    /// Who can issue <see cref="Permission"/>, as a noun phrase usable mid
    /// sentence - for example <c>a platform administrator</c>.
    /// <see langword="null"/> when nobody specific can be named.
    /// </summary>
    public string? Audience { get; }

    /// <summary>
    /// Whether this remedy names both a missing grant and someone who can issue
    /// it, and is therefore worth rendering.
    /// </summary>
    public bool IsSpecified => Permission is { Length: > 0 } && Audience is { Length: > 0 };

    /// <summary>
    /// Returns the remedy for a caller missing <paramref name="permission"/>,
    /// obtainable from <paramref name="audience"/>.
    /// </summary>
    /// <param name="permission">
    /// The missing grant, named as the cluster names it. Must not be
    /// <see langword="null"/> or empty.
    /// </param>
    /// <param name="audience">
    /// Who can issue it, as a noun phrase. Must not be <see langword="null"/> or
    /// empty.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="permission"/> or <paramref name="audience"/> is
    /// <see langword="null"/> or empty.
    /// </exception>
    public static ExplorerAccessRemedy Requiring(string permission, string audience)
    {
        ArgumentException.ThrowIfNullOrEmpty(permission);
        ArgumentException.ThrowIfNullOrEmpty(audience);
        return new ExplorerAccessRemedy(permission, audience);
    }

    /// <summary>
    /// Composes the plain-language remedy sentence, or <see langword="null"/>
    /// when this remedy names no grant.
    /// <para>
    /// Deliberately <em>not</em> called on the probe path: a gate resolves
    /// hundreds of times per session and the string is needed only where one is
    /// rendered. A shell with its own copy layer should read
    /// <see cref="Permission"/> and <see cref="Audience"/> and phrase its own.
    /// </para>
    /// </summary>
    /// <returns>The remedy sentence, or <see langword="null"/>.</returns>
    public string? Describe() =>
        IsSpecified ? $"Requires the {Permission} permission - ask {Audience}." : null;
}
