using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Default <see cref="IExplorerPreferenceScopeProvider"/>: the signed-in user
/// from <see cref="IExplorerAuthSession"/> and the connected cluster from
/// <see cref="IExplorerSession"/>, re-read whenever either announces a change.
/// </summary>
/// <remarks>
/// Both dependencies are optional. A head or a test that registers the session
/// stores without an auth session or a configured connection still gets a
/// working provider, scoped to the signed-out, unconfigured identity - which
/// degrades to "one shared scope" rather than failing to resolve.
/// </remarks>
public sealed class ExplorerPreferenceScopeProvider : IExplorerPreferenceScopeProvider, IDisposable
{
    private readonly IExplorerAuthSession? _auth;
    private readonly IExplorerSession? _session;
    private ExplorerPreferenceScopeIdentity _current;

    /// <summary>
    /// Creates the provider over the session's identity sources.
    /// </summary>
    /// <param name="auth">The sign-in state, or <see langword="null"/> when the head registers none.</param>
    /// <param name="session">The connection configuration, or <see langword="null"/> when the head registers none.</param>
    public ExplorerPreferenceScopeProvider(IExplorerAuthSession? auth, IExplorerSession? session)
    {
        _auth = auth;
        _session = session;
        _current = Read();

        if (_auth is not null)
        {
            _auth.AuthenticationChanged += OnSourceChanged;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged += OnSourceChanged;
        }
    }

    /// <inheritdoc />
    public ExplorerPreferenceScopeIdentity Current => _current;

    /// <inheritdoc />
    public event Action? ScopeChanged;

    /// <summary>Detaches from the identity sources.</summary>
    public void Dispose()
    {
        if (_auth is not null)
        {
            _auth.AuthenticationChanged -= OnSourceChanged;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged -= OnSourceChanged;
        }
    }

    private void OnSourceChanged()
    {
        var next = Read();
        if (next == _current)
        {
            // A sign-in that did not change the username, or a configuration
            // apply that kept the endpoint, moves no preferences.
            return;
        }

        _current = next;
        ScopeChanged?.Invoke();
    }

    private ExplorerPreferenceScopeIdentity Read()
    {
        var user = _auth?.Username;
        var cluster = _session?.Current?.Endpoint;

        return new ExplorerPreferenceScopeIdentity(
            string.IsNullOrEmpty(user) ? ExplorerPreferenceScopeIdentity.Anonymous : user,
            string.IsNullOrEmpty(cluster) ? ExplorerPreferenceScopeIdentity.Unconfigured : cluster);
    }
}
