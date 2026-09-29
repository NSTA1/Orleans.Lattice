using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// One circuit's gRPC connection to the cluster: the single channel every Shell
/// transport adapter in that circuit calls over, carrying that circuit's sign-in.
/// </summary>
/// <remarks>
/// <para>
/// <b>Scoped per circuit.</b> The channel reads the endpoint from the circuit's
/// <see cref="IExplorerSession"/> and the credential from the circuit's
/// <see cref="IExplorerAuthSession"/>, both scoped, so one operator's credential
/// can never reach another circuit. It must never be captured by a singleton.
/// </para>
/// <para>
/// <b>The credential goes through the Core plumbing.</b> The settings come from
/// <see cref="ExplorerConfiguration.ToConnectionSettings"/> with
/// <see cref="IExplorerAuthSession.CurrentAuthentication"/> applied, and the call
/// invoker is built by <see cref="LatticeGrpcChannelFactory.CreateCallInvoker"/>,
/// so the transport headers, the static-credential transport gate and the
/// per-call token provider behave exactly as they do for the state connection.
/// Every facade rides the one configured endpoint, as the plugin adapters did.
/// </para>
/// <para>
/// <b>Rebuilt lazily.</b> The channel is rebuilt on the next call after the
/// configuration or the sign-in changes (both are immutable instances, compared
/// by reference, so a steady-state call allocates nothing here). A call still in
/// flight on the replaced channel fails when that channel is disposed.
/// </para>
/// <para>
/// <b>The tenant is asserted per call, never built in.</b> The circuit's
/// <see cref="ILatticeActiveTenantProvider"/> rides the settings and is asked for
/// the tenant as each call starts, so a tenant switch changes the next call
/// without rebuilding the channel, and a channel can never carry one circuit's
/// tenant into another's calls.
/// </para>
/// </remarks>
internal sealed class ShellTransportChannel : IDisposable
{
    /// <summary>The message a call made before an endpoint is configured fails with.</summary>
    internal const string NotConfiguredMessage = "The explorer is not configured with an endpoint yet.";

    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private readonly IShellGrpcChannelFactory _channelFactory;
    private readonly ShellTransportSerializer _serializer;
    private readonly ILatticeActiveTenantProvider? _activeTenant;
    private readonly object _gate = new();

    private GrpcChannel? _channel;
    private CallInvoker? _invoker;
    private ExplorerConfiguration? _builtConfiguration;
    private LatticeCallAuthentication? _builtAuthentication;
    private bool _disposed;

    /// <summary>Creates the circuit's channel.</summary>
    /// <param name="session">The circuit's explorer session, which owns the endpoint.</param>
    /// <param name="auth">The circuit's auth session, whose current sign-in is attached.</param>
    /// <param name="channelFactory">Builds the underlying gRPC channel.</param>
    /// <param name="serializer">The shared Orleans serializer provider.</param>
    /// <param name="activeTenant">
    /// The circuit's live tenant source, asked on every call for the tenant to
    /// assert; <see langword="null"/> when the head registers no tenancy, so no
    /// call carries a tenant.
    /// </param>
    /// <exception cref="ArgumentNullException">Any required argument is <see langword="null"/>.</exception>
    public ShellTransportChannel(
        IExplorerSession session,
        IExplorerAuthSession auth,
        IShellGrpcChannelFactory channelFactory,
        ShellTransportSerializer serializer,
        ILatticeActiveTenantProvider? activeTenant = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        ArgumentNullException.ThrowIfNull(channelFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _session = session;
        _auth = auth;
        _channelFactory = channelFactory;
        _serializer = serializer;
        _activeTenant = activeTenant;
        Invoker = new ShellCircuitCallInvoker(this);
    }

    /// <summary>
    /// The invoker a typed gRPC client is built over. It resolves the current
    /// channel on every call, so a client built once follows every reconfiguration
    /// and sign-in for the life of the circuit.
    /// </summary>
    public CallInvoker Invoker { get; }

    /// <summary>A service provider with Orleans serialization registered, for building typed clients.</summary>
    public IServiceProvider SerializerServices => _serializer.Services;

    /// <summary>
    /// Returns the invoker for the current endpoint and sign-in, rebuilding the
    /// channel when either has changed since it was last built.
    /// </summary>
    /// <returns>The current invoker.</returns>
    /// <exception cref="InvalidOperationException">
    /// No endpoint is configured, or the Core static-credential gate refuses to
    /// send the sign-in over a plaintext endpoint that was not opted in.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The circuit has ended.</exception>
    internal CallInvoker ResolveInvoker()
    {
        var configuration = _session.Current ?? throw new InvalidOperationException(NotConfiguredMessage);
        var authentication = _auth.CurrentAuthentication;

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_invoker is not null
                && ReferenceEquals(_builtConfiguration, configuration)
                && ReferenceEquals(_builtAuthentication, authentication))
            {
                return _invoker;
            }

            var settings = configuration.ToConnectionSettings() with
            {
                Authentication = authentication,
                ActiveTenantProvider = _activeTenant,
            };
            var channel = _channelFactory.CreateChannel(settings);
            CallInvoker invoker;
            try
            {
                invoker = LatticeGrpcChannelFactory.CreateCallInvoker(channel, settings);
            }
            catch
            {
                channel.Dispose();
                throw;
            }

            _channel?.Dispose();
            _channel = channel;
            _invoker = invoker;
            _builtConfiguration = configuration;
            _builtAuthentication = authentication;
            return invoker;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _channel?.Dispose();
            _channel = null;
            _invoker = null;
            _builtConfiguration = null;
            _builtAuthentication = null;
        }
    }
}
