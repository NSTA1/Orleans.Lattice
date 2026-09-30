using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// One simulated circuit for the Shell transport tests: the real Shell
/// registration (<c>AddLatticeExplorerShell</c>), the circuit's session and
/// auth-session substitutes, and the in-memory <see cref="ShellTransportPeer"/>
/// under every channel. The container validates scopes and every registration
/// on build, so a singleton that captured a scoped transport would fail here.
/// </summary>
internal sealed class ShellTransportCircuit : IDisposable
{
    /// <summary>A loopback h2c endpoint; the peer answers, so nothing listens on it.</summary>
    public const string Endpoint = "http://localhost:1";

    /// <summary>
    /// One codec provider for every simulated circuit. It holds no circuit state -
    /// which is why production registers it as a singleton - and building it is the
    /// dominant cost of a circuit, so the tests share it. It is registered as an
    /// instance, so no circuit's container disposes it.
    /// </summary>
    private static readonly ShellTransportSerializer SharedSerializer = new();

    private readonly ServiceProvider _root;
    private readonly IServiceScope _scope;
    private readonly List<IServiceScope> _siblings = [];

    /// <summary>Builds a circuit whose endpoint is configured and whose user is signed out.</summary>
    /// <param name="configure">Registers extra services before the Shell registers its own.</param>
    public ShellTransportCircuit(Action<IServiceCollection>? configure = null)
    {
        Session.Current.Returns(_ => Configuration);
        Auth.CurrentAuthentication.Returns(_ => Authentication);
        Auth.GetAuthenticationFor(Arg.Any<string>()).Returns(call =>
            string.Equals(call.Arg<string>()?.TrimEnd('/'), AuthenticationEndpoint?.TrimEnd('/'), StringComparison.OrdinalIgnoreCase)
                ? Authentication
                : null);
        ChannelFactory = new ShellTransportPeerChannelFactory(Peer);

        var services = new ServiceCollection();
        services.AddScoped(_ => Session);
        services.AddScoped(_ => Auth);
        services.AddSingleton<IShellGrpcChannelFactory>(ChannelFactory);
        services.AddSingleton(SharedSerializer);
        services.AddShellTransportTestHead();
        configure?.Invoke(services);
        services.AddLatticeExplorerShell();

        _root = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        _scope = _root.CreateScope();
    }

    /// <summary>The in-memory peer.</summary>
    public ShellTransportPeer Peer { get; } = new();

    /// <summary>The channel factory, which records every channel built.</summary>
    public ShellTransportPeerChannelFactory ChannelFactory { get; }

    /// <summary>The circuit's explorer session substitute.</summary>
    public IExplorerSession Session { get; } = Substitute.For<IExplorerSession>();

    /// <summary>The circuit's auth session substitute.</summary>
    public IExplorerAuthSession Auth { get; } = Substitute.For<IExplorerAuthSession>();

    /// <summary>The configuration the session reports; <see langword="null"/> models an unconfigured Explorer.</summary>
    public ExplorerConfiguration? Configuration { get; set; } = PlaintextConfiguration();

    /// <summary>The sign-in the auth session reports; <see langword="null"/> is signed out.</summary>
    public LatticeCallAuthentication? Authentication { get; set; }

    /// <summary>The endpoint <see cref="Authentication"/> was minted for; <see cref="Endpoint"/> unless a test repoints it.</summary>
    public string? AuthenticationEndpoint { get; set; } = Endpoint;

    /// <summary>The circuit's scoped services.</summary>
    public IServiceProvider Services => _scope.ServiceProvider;

    /// <summary>The loopback h2c configuration, opted in to plaintext.</summary>
    /// <param name="transportHeaders">Optional non-secret transport headers.</param>
    /// <returns>The configuration.</returns>
    public static ExplorerConfiguration PlaintextConfiguration(IReadOnlyDictionary<string, string>? transportHeaders = null) => new()
    {
        Endpoint = Endpoint,
        AllowUnencryptedHttp2 = true,
        TransportMode = ExplorerTransportMode.InsecureLoopbackDev,
        TransportHeaders = transportHeaders,
    };

    /// <summary>Resolves <typeparamref name="TFacade"/> and teaches the peer its RPCs.</summary>
    /// <typeparam name="TFacade">The facade interface.</typeparam>
    /// <returns>The circuit's adapter.</returns>
    public TFacade Resolve<TFacade>()
        where TFacade : class => Resolve<TFacade>(Services);

    /// <summary>
    /// Opens another circuit on the same host: its own scope, so its own channel,
    /// adapters and tenant context, over the same singletons and the same peer.
    /// </summary>
    /// <returns>The sibling circuit's scoped services; disposed with this circuit.</returns>
    public IServiceProvider CreateSibling()
    {
        var scope = _root.CreateScope();
        _siblings.Add(scope);
        return scope.ServiceProvider;
    }

    /// <summary>Resolves <typeparamref name="TFacade"/> in <paramref name="services"/> and teaches the peer its RPCs.</summary>
    /// <typeparam name="TFacade">The facade interface.</typeparam>
    /// <param name="services">A circuit's scoped services, from <see cref="Services"/> or <see cref="CreateSibling"/>.</param>
    /// <returns>That circuit's adapter.</returns>
    public TFacade Resolve<TFacade>(IServiceProvider services)
        where TFacade : class
    {
        var facade = services.GetRequiredKeyedService<TFacade>(ShellFacades.Key);
        Peer.Serializers = services.GetRequiredService<ShellTransportSerializer>().Services;
        Peer.Learn(facade);
        return facade;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var sibling in _siblings)
        {
            sibling.Dispose();
        }

        _scope.Dispose();
        _root.Dispose();
        Peer.Dispose();
    }
}
