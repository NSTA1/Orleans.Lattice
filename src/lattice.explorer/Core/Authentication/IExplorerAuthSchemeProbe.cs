namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Discovers how a state-API endpoint wants callers to authenticate by making
/// the unauthenticated auth-scheme advertisement probe against it. The probe is
/// deliberately credential-free: it must succeed before the user has any
/// credential, so it can only ever return the endpoint's public advertisement
/// (schemes plus public OIDC parameters) and never leaks protected data.
/// </summary>
public interface IExplorerAuthSchemeProbe
{
    /// <summary>
    /// Probes <paramref name="address"/> for its advertised auth schemes. Returns
    /// <see cref="ExplorerAuthSchemeAdvertisement.Empty"/> when the endpoint does
    /// not advertise (an older server) or the probe cannot reach it, so the
    /// caller falls back to manual scheme selection rather than failing.
    /// </summary>
    /// <param name="address">The state-API endpoint address.</param>
    /// <param name="allowUnencryptedHttp2">Whether to permit an <c>http://</c> (h2c) endpoint.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2 = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes <paramref name="address"/> for its advertised auth schemes, attaching
    /// <paramref name="transportHeaders"/> to the call. The probe runs before the
    /// user has any credential, so these non-secret routing headers (for example an
    /// origin-routing header a fronting proxy requires) are the only thing that can
    /// gate it; without them a proxy-guarded endpoint rejects the unauthenticated
    /// probe and discovery wrongly degrades to manual/Basic. The default
    /// implementation forwards to <see cref="ProbeAsync(string, bool, CancellationToken)"/>
    /// so an existing probe that predates transport headers keeps working (it
    /// simply cannot reach a header-gated endpoint).
    /// </summary>
    /// <param name="address">The state-API endpoint address.</param>
    /// <param name="allowUnencryptedHttp2">Whether to permit an <c>http://</c> (h2c) endpoint.</param>
    /// <param name="transportHeaders">Non-secret transport headers to attach to the probe call, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2,
        IReadOnlyDictionary<string, string>? transportHeaders,
        CancellationToken cancellationToken = default)
        => ProbeAsync(address, allowUnencryptedHttp2, cancellationToken);
}
