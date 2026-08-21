using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// Shared scaffolding for the production gRPC control-client tests
/// (<c>GrpcAuthAdminClient</c>, <c>GrpcSchemaAdminClient</c>,
/// <c>GrpcBackupControlClient</c>). These clients build a real gRPC channel to the
/// configured endpoint but never reach a server in a unit test: every call is made
/// with an already-cancelled token so the transport fails fast and deterministically
/// with a <see cref="Grpc.Core.RpcException"/> whose status is
/// <see cref="Grpc.Core.StatusCode.Cancelled"/>. That exercises the client's channel
/// build, credential wiring, and cancellation plumbing without standing up a cluster.
/// </summary>
internal static class ExplorerControlClientHarness
{
    /// <summary>A loopback h2c endpoint that no server listens on.</summary>
    public const string LoopbackHttp = "http://localhost:1";

    /// <summary>A loopback TLS endpoint that no server listens on.</summary>
    public const string LoopbackHttps = "https://localhost:1";

    /// <summary>
    /// Builds a substitute <see cref="IExplorerSession"/> whose
    /// <see cref="IExplorerSession.Current"/> returns <paramref name="configuration"/>
    /// (or <see langword="null"/> to model an unconfigured explorer).
    /// </summary>
    public static IExplorerSession Session(ExplorerConfiguration? configuration)
    {
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(configuration);
        return session;
    }

    /// <summary>
    /// Builds a substitute <see cref="IExplorerAuthSession"/> whose
    /// <see cref="IExplorerAuthSession.CurrentAuthentication"/> returns
    /// <paramref name="authentication"/>.
    /// </summary>
    public static IExplorerAuthSession Auth(LatticeCallAuthentication? authentication)
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.CurrentAuthentication.Returns(authentication);
        return auth;
    }

    /// <summary>A configuration for a plaintext (h2c) loopback endpoint.</summary>
    public static ExplorerConfiguration H2cConfig(
        IReadOnlyDictionary<string, string>? headers = null,
        IReadOnlyDictionary<string, string>? transportHeaders = null) => new()
        {
            Endpoint = LoopbackHttp,
            AllowUnencryptedHttp2 = true,
            TransportMode = ExplorerTransportMode.InsecureLoopbackDev,
            Headers = headers,
            TransportHeaders = transportHeaders,
        };

    /// <summary>A configuration for a TLS loopback endpoint.</summary>
    public static ExplorerConfiguration TlsConfig() => new()
    {
        Endpoint = LoopbackHttps,
        AllowUnencryptedHttp2 = false,
        TransportMode = ExplorerTransportMode.Secure,
    };

    /// <summary>An already-cancelled token so every call fails fast without a server.</summary>
    public static CancellationToken Cancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }
}
