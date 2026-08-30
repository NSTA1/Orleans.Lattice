using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The single place the Explorer builds a gRPC channel and its call invoker.
/// Every gRPC client in every Explorer package - the state connection, the
/// unauthenticated auth-scheme probe, and each plugin's control-plane client -
/// routes through here, so the transport, the insecure-channel safeguard, and
/// the credential pipeline are decided once rather than copied per client.
/// <para>
/// <b>Why this type exists.</b> Each new gRPC client used to be written by
/// copying an established sibling, which propagated a per-circuit
/// <see cref="AppContext.SetSwitch(string, bool)"/> call that mutated
/// <em>process-global</em> state on behalf of one endpoint's opt-in. Channel
/// construction lives here now so the next client cannot reintroduce it by
/// copying a sibling, and
/// <c>AppContextSwitchHygieneTests</c> fails the build if one tries.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Unencrypted HTTP/2 (h2c) is a per-channel concern.</b> Each channel gets
/// its own <see cref="SocketsHttpHandler"/> - configured exactly as
/// <c>Grpc.Net.Client</c> configures the one it would otherwise create for
/// itself - and the channel owns its disposal. Nothing about one channel's
/// transport can therefore leak onto another channel in the same process, which
/// matters on the Blazor Server head where circuits are per-browser and share
/// one process.
/// </para>
/// <para>
/// The <c>System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport</c> app
/// switch the Explorer used to set is a .NET Core 3.x artefact that the modern
/// runtime ignores outright: <see cref="SocketsHttpHandler"/> negotiates h2c by
/// prior knowledge whenever the request asks for HTTP/2 over an <c>http</c>
/// address, which is what <c>Grpc.Net.Client</c> always does. Dropping the
/// switch therefore changes no transport behaviour; it only stops one circuit's
/// choice being written into shared process state.
/// </para>
/// <para>
/// <see cref="LatticeConnectionSettings.AllowUnencryptedHttp2"/> keeps the job
/// that is genuinely enforced: together with a plaintext address and a live
/// credential provider it is what lifts gRPC's insecure-channel safeguard. That
/// gate is unchanged and is deliberately not widened here.
/// </para>
/// </remarks>
public static class LatticeGrpcChannelFactory
{
    /// <summary>
    /// Builds the channel for <paramref name="settings"/> over the options
    /// <see cref="BuildChannelOptions"/> resolves - the insecure-channel
    /// safeguard, and a transport handler scoped to this channel alone.
    /// </summary>
    /// <param name="settings">The endpoint settings. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// The channel. The caller owns it and must dispose it; disposing it also
    /// disposes the transport handler built for it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="settings"/> is <see langword="null"/>.</exception>
    public static GrpcChannel CreateChannel(LatticeConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return GrpcChannel.ForAddress(settings.Address, BuildChannelOptions(settings));
    }

    /// <summary>
    /// Builds the call invoker for <paramref name="channel"/>: the
    /// sign-in-independent transport headers, then the authentication the
    /// settings carry.
    /// </summary>
    /// <remarks>
    /// Transport headers accompany every call regardless of the auth mode (for
    /// example an origin-routing header a fronting proxy requires), so they are
    /// applied before, and independently of, the authentication interceptor - a
    /// sign-in replaces <see cref="LatticeConnectionSettings.Authentication"/>
    /// but never <see cref="LatticeConnectionSettings.TransportHeaders"/>.
    /// </remarks>
    /// <param name="channel">The channel to invoke over. Must not be <see langword="null"/>.</param>
    /// <param name="settings">The endpoint settings. Must not be <see langword="null"/>.</param>
    /// <returns>The invoker, with headers and credentials attached per call.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public static CallInvoker CreateCallInvoker(GrpcChannel channel, LatticeConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(settings);

        var invoker = ApplyTransportHeaders(channel.CreateCallInvoker(), settings.TransportHeaders);
        return ApplyAuthentication(invoker, settings.Authentication);
    }

    /// <summary>
    /// Applies the sign-in-independent transport headers (if any) to every call
    /// on <paramref name="invoker"/>, returning it unchanged when there are none.
    /// </summary>
    /// <param name="invoker">The invoker to decorate. Must not be <see langword="null"/>.</param>
    /// <param name="transportHeaders">The headers to attach, or <see langword="null"/>.</param>
    /// <returns>The decorated invoker, or <paramref name="invoker"/> unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="invoker"/> is <see langword="null"/>.</exception>
    public static CallInvoker ApplyTransportHeaders(
        CallInvoker invoker,
        IReadOnlyDictionary<string, string>? transportHeaders)
    {
        ArgumentNullException.ThrowIfNull(invoker);

        if (transportHeaders is not { Count: > 0 } headers)
        {
            return invoker;
        }

        return invoker.Intercept(metadata =>
        {
            foreach (var (key, value) in headers)
            {
                metadata.Add(key, value);
            }

            return metadata;
        });
    }

    /// <summary>
    /// Builds the <see cref="GrpcChannelOptions"/> for <paramref name="settings"/>:
    /// the transport handler and the insecure-channel safeguard, which is the
    /// whole of how an Explorer channel is configured.
    /// <para>
    /// <b>The transport handler is this channel's own.</b> Each call allocates a
    /// fresh <see cref="SocketsHttpHandler"/> - configured exactly as
    /// <c>Grpc.Net.Client</c> configures the one it would otherwise create for
    /// itself - and sets
    /// <see cref="GrpcChannelOptions.DisposeHttpClient"/> so the channel disposes
    /// it. Supplying a handler suppresses gRPC's own default-disposal rule, so
    /// without that flag every rebuild on an endpoint or sign-in change would
    /// leak a connection pool. On a platform without
    /// <see cref="SocketsHttpHandler"/> (Blazor WebAssembly) the handler is left
    /// unset, deferring to the same fallback gRPC would have chosen.
    /// </para>
    /// <para>
    /// <b>The insecure-channel safeguard.</b> gRPC refuses to send per-call
    /// credentials over a channel it cannot confirm is secure; that safeguard
    /// (<see cref="GrpcChannelOptions.UnsafeUseInsecureChannelCallCredentials"/>
    /// set to <see langword="true"/>) is only lifted when the endpoint is
    /// genuinely plaintext (an <c>http</c> address) AND the operator has
    /// explicitly opted into unencrypted transport via
    /// <see cref="LatticeConnectionSettings.AllowUnencryptedHttp2"/>. For an
    /// <c>https</c> endpoint the safeguard is left active (the flag stays
    /// <see langword="false"/>) so credentials are never sent over a channel gRPC
    /// cannot verify; credentials still attach over the confirmed-secure TLS
    /// channel through the call-credentials interceptor. This mirrors the
    /// replication transport's scheme gate.
    /// </para>
    /// </summary>
    /// <param name="settings">The endpoint settings. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// The options, with the safeguard resolved for this endpoint and a transport
    /// handler the resulting channel will own.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="settings"/> is <see langword="null"/>.</exception>
    public static GrpcChannelOptions BuildChannelOptions(LatticeConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var channelOptions = new GrpcChannelOptions();

        if (SocketsHttpHandler.IsSupported)
        {
            // EnableMultipleHttp2Connections lets a channel open another
            // connection once the peer's max concurrent streams is reached, which
            // is what Grpc.Net.Client's own primary handler does - so supplying
            // one changes nothing but its lifetime and its scope.
            channelOptions.HttpHandler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true };
            channelOptions.DisposeHttpClient = true;
        }

        if (settings.Authentication is { HasCredentialProvider: true }
            && settings.AllowUnencryptedHttp2
            && !IsHttpsAddress(settings.Address))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
        }

        return channelOptions;
    }

    /// <summary>
    /// Attaches <paramref name="authentication"/> to every call: a live token
    /// provider consulted per RPC, or the static headers a non-interactive
    /// sign-in supplied. An anonymous connection is returned undecorated.
    /// </summary>
    private static CallInvoker ApplyAuthentication(CallInvoker invoker, LatticeCallAuthentication? authentication)
    {
        if (authentication is { HasCredentialProvider: true, CredentialProvider: { } provider })
        {
            // CallCredentials.FromInterceptor is invoked per RPC and may await, so
            // the provider can refresh a near-expiry token before the header is
            // written. The token is never captured statically on the channel.
            var callCredentials = CallCredentials.FromInterceptor(async (context, metadata) =>
            {
                var header = await provider
                    .GetAuthorizationHeaderAsync(context.CancellationToken)
                    .ConfigureAwait(false);
                if (!string.IsNullOrEmpty(header))
                {
                    metadata.Add(LatticeCallAuthentication.AuthorizationHeaderName, header);
                }
            });

            return invoker.Intercept(new CallCredentialsInterceptor(callCredentials));
        }

        if (authentication is { HasHeaders: true })
        {
            var headers = authentication.Headers!;
            return invoker.Intercept(metadata =>
            {
                foreach (var (key, value) in headers)
                {
                    metadata.Add(key, value);
                }

                return metadata;
            });
        }

        return invoker;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="address"/> is an
    /// absolute <c>https</c> URI. A non-absolute or non-https address is treated
    /// as non-https so the insecure-channel safeguard is only ever lifted for an
    /// endpoint confirmed to be plaintext.
    /// </summary>
    private static bool IsHttpsAddress(string? address) =>
        Uri.TryCreate(address, UriKind.Absolute, out var uri)
        && string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
}
