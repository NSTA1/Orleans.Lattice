namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// What an endpoint advertised about how to authenticate to it: the ordered set
/// of schemes it accepts. An empty advertisement means the endpoint did not
/// advertise (an older server, or the probe was unreachable), in which case the
/// explorer falls back to a manually-selected or Basic scheme.
/// </summary>
public sealed record ExplorerAuthSchemeAdvertisement
{
    /// <summary>An empty advertisement (nothing was advertised).</summary>
    public static readonly ExplorerAuthSchemeAdvertisement Empty = new();

    /// <summary>The advertised schemes, in the server's preference order.</summary>
    public IReadOnlyList<ExplorerAuthSchemeDescriptor> Schemes { get; init; } =
        Array.Empty<ExplorerAuthSchemeDescriptor>();

    /// <summary><see langword="true"/> when the endpoint advertised at least one scheme.</summary>
    public bool HasSchemes => Schemes.Count > 0;
}
