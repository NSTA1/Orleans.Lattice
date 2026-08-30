using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// The web head's default <see cref="IExplorerConfigStore"/>: reads delegate to the
/// inner store, writes are refused.
/// </summary>
/// <remarks>
/// <para>
/// The persisted configuration names the cluster endpoint every circuit dials and
/// every sign-in is challenged against. In the desktop head that file is per-user,
/// so the operator editing it is the only party affected. In the web head it is a
/// single process-wide document shared by every browser, and nothing upstream
/// authenticates the caller who writes it: an anonymous visitor could repoint the
/// whole head at a host they control, then collect the next operator's cluster
/// credential when the login endpoint challenges the attacker's endpoint, and have
/// the server dial that host for every catalog, metrics, and topology read.
/// </para>
/// <para>
/// Refusing the write at the store closes that at the single seam every writer
/// funnels through, rather than at one caller that a future caller could bypass.
/// A web head is configured out of band - by <c>LATTICE_EXPLORER_ENDPOINT</c> (the
/// default-on environment bootstrap) or by a pre-provisioned document named by
/// <see cref="LatticeExplorerWebOptions.ConfigFilePath"/> / <c>LATTICE_EXPLORER_CONFIG</c>
/// - so no configuration channel is lost. A deployment that genuinely wants
/// browser-driven configuration opts back in with
/// <see cref="LatticeExplorerWebOptions.AllowInteractiveEndpointConfiguration"/>.
/// </para>
/// </remarks>
internal sealed class ReadOnlyExplorerConfigStore : IExplorerConfigStore
{
    /// <summary>The message carried by the refusal, surfaced inline by the configuration dialog.</summary>
    internal const string RefusalMessage =
        "This Orleans.Lattice Explorer head does not accept endpoint configuration from the browser. "
        + "The connection endpoint is set by the deployment through the LATTICE_EXPLORER_ENDPOINT environment "
        + "variable or a pre-provisioned configuration document. Set "
        + nameof(LatticeExplorerWebOptions) + "." + nameof(LatticeExplorerWebOptions.AllowInteractiveEndpointConfiguration)
        + " to allow it.";

    private readonly IExplorerConfigStore _inner;

    /// <summary>Wraps <paramref name="inner"/>, exposing its reads and refusing its writes.</summary>
    /// <param name="inner">The real backing store reads delegate to.</param>
    public ReadOnlyExplorerConfigStore(IExplorerConfigStore inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    /// <inheritdoc />
    public string FilePath => _inner.FilePath;

    /// <inheritdoc />
    public bool Exists => _inner.Exists;

    /// <inheritdoc />
    public Task<ExplorerConfiguration?> LoadAsync(CancellationToken cancellationToken = default)
        => _inner.LoadAsync(cancellationToken);

    /// <inheritdoc />
    public Task SaveAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        throw new InvalidOperationException(RefusalMessage);
    }
}
