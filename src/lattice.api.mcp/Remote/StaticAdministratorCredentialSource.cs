using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The default <see cref="ILatticeApiMcpAdministratorCredentialSource"/>: returns
/// the static <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/>
/// configured on the remote options, or <see langword="null"/> when none is set.
/// This preserves the historical behaviour in which a host supplies a fixed
/// administrator token; hosts that need a self-refreshing managed-identity token
/// register <see cref="ManagedIdentityAdministratorCredentialSource"/> instead via
/// <see cref="LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions.AddLatticeMcpManagedIdentityAdministrator"/>.
/// </summary>
internal sealed class StaticAdministratorCredentialSource : ILatticeApiMcpAdministratorCredentialSource
{
    private readonly IOptionsMonitor<LatticeApiMcpRemoteOptions> _options;

    /// <summary>Initialises the source over the monitored remote options.</summary>
    public StaticAdministratorCredentialSource(IOptionsMonitor<LatticeApiMcpRemoteOptions> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public LatticeCredential? Resolve() => _options.CurrentValue.AdministratorCredential;
}
