using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The circuit's caller, as the key every per-circuit memo of a cluster answer is
/// filed under (<see cref="ShellCallerKey"/>). A memo stores the key it was read
/// under, serves itself only while <see cref="Current"/> still equals it, and is
/// written back only when the caller did not change while it was read.
/// </summary>
/// <remarks>
/// <para>
/// This is the one place a memo learns who it belongs to, so no memo can key on
/// the tenant and forget the identity or the endpoint, which is how an answer
/// read for one caller came to be served to the next one in the same circuit
/// (issue #4019). <c>CallerKeyedMemoHygieneTests</c> fails any per-circuit
/// service that remembers state without it.
/// </para>
/// <para>
/// It counts every <see cref="IExplorerAuthSession.AuthenticationChanged"/> and
/// <see cref="IExplorerSession.ConfigurationChanged"/> into the key's generation,
/// so even two identities that share a display name never share a memo.
/// </para>
/// </remarks>
internal sealed class ShellCaller : IDisposable
{
    private readonly IExplorerAuthSession? _auth;
    private readonly IExplorerSession? _session;
    private readonly ILatticeActiveTenantProvider? _tenant;
    private readonly bool _observing;
    private long _generation;

    /// <summary>Reads the caller from the circuit's sessions.</summary>
    /// <param name="auth">The circuit's sign-in, or <see langword="null"/> when the head has none.</param>
    /// <param name="session">The circuit's connection, or <see langword="null"/> when the head has none.</param>
    /// <param name="tenant">The tenant the circuit's calls assert, or <see langword="null"/> when tenancy is off.</param>
    public ShellCaller(IExplorerAuthSession? auth = null, IExplorerSession? session = null, ILatticeActiveTenantProvider? tenant = null)
        : this(auth, session, tenant, observe: true)
    {
    }

    private ShellCaller(IExplorerAuthSession? auth, IExplorerSession? session, ILatticeActiveTenantProvider? tenant, bool observe)
    {
        _auth = auth;
        _session = session;
        _tenant = tenant;
        _observing = observe;
        if (!observe)
        {
            return;
        }

        if (_auth is not null)
        {
            _auth.AuthenticationChanged += OnChanged;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged += OnChanged;
        }
    }

    /// <summary>The caller now. Allocates nothing.</summary>
    public ShellCallerKey Current => new(
        _auth?.IsAuthenticated == true,
        _auth?.CurrentScheme,
        _auth?.Username,
        _session?.Current?.Endpoint,
        _tenant?.AssertedTenant,
        Volatile.Read(ref _generation));

    /// <summary>
    /// The circuit's caller: the registered one, or, in a host that registers
    /// none, one read from whichever of the circuit's sessions exist. That fallback
    /// does not subscribe to the sessions, since nothing would ever dispose it: it
    /// keys on the sign-in, the user, the endpoint and the tenant, with no generation.
    /// </summary>
    /// <param name="services">The circuit's services.</param>
    /// <returns>The caller.</returns>
    public static ShellCaller Of(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.GetService<ShellCaller>() ?? new ShellCaller(
            services.GetService<IExplorerAuthSession>(),
            services.GetService<IExplorerSession>(),
            services.GetService<ShellAssertedTenant>() ?? services.GetService<ILatticeActiveTenantProvider>(),
            observe: false);
    }

    /// <summary>
    /// A caller over the given sessions that does not subscribe to them, for an
    /// owner that cannot dispose one: it keys on the sign-in, the user, the endpoint
    /// and the tenant, with no generation.
    /// </summary>
    /// <param name="auth">The circuit's sign-in, or <see langword="null"/>.</param>
    /// <param name="session">The circuit's connection, or <see langword="null"/>.</param>
    /// <param name="tenant">The tenant the circuit's calls assert, or <see langword="null"/>.</param>
    /// <returns>The caller.</returns>
    internal static ShellCaller Unobserved(IExplorerAuthSession? auth = null, IExplorerSession? session = null, ILatticeActiveTenantProvider? tenant = null) =>
        new(auth, session, tenant, observe: false);

    /// <summary>Builds a caller over the circuit's sessions and its asserted tenant.</summary>
    /// <param name="services">The circuit's services.</param>
    /// <returns>A new caller.</returns>
    internal static ShellCaller Create(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return new ShellCaller(
            services.GetService<IExplorerAuthSession>(),
            services.GetService<IExplorerSession>(),
            services.GetService<ShellAssertedTenant>() ?? services.GetService<ILatticeActiveTenantProvider>());
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_observing)
        {
            return;
        }

        if (_auth is not null)
        {
            _auth.AuthenticationChanged -= OnChanged;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged -= OnChanged;
        }
    }

    private void OnChanged() => Interlocked.Increment(ref _generation);
}
