using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// Pins the security property the T2 hoist could have silently broken: the
/// Azure-minted <b>backend</b> token stays scoped to the telemetry backend and did
/// not become reachable from another caller as a side effect of the PromQL
/// machinery moving packages (and its types becoming <see langword="public"/>).
/// </summary>
/// <remarks>
/// <para>
/// The hoist introduced a genuinely new hazard that a compile-only re-point does
/// not exercise. <c>AddTelemetryTools</c> now forwards
/// <c>IOptions&lt;LatticeTelemetryOptions&gt;</c> to the <b>very same instance</b>
/// the binding's own <see cref="LatticeApiMcpTelemetryOptions"/> resolves to, so
/// that one object is read by a much wider audience than before - every MCP tool
/// handler among them. Were the proxy ever to cache the minted token onto
/// <see cref="LatticeTelemetryOptions.Credential"/>, the Azure credential would
/// become readable by anything holding either options type. These tests drive the
/// real registered proxy over a capturing transport and assert the token appears
/// on the outbound backend request and nowhere else.
/// </para>
/// <para>
/// Every assertion is deterministic: a fake credential mints a fixed sentinel with
/// a far-future expiry, so nothing here depends on wall-clock time, ordering, or
/// timing.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AzureTelemetryCredentialIsolationTests
{
    private const string BackendBase = "https://workspace.eastus.prometheus.monitor.azure.com/";
    private const string Sentinel = "azure-minted-sentinel-token";
    private const string StaticToken = "host-configured-static-token";

    /// <summary>
    /// Composes the real host wiring an MCP deployment uses - the MCP telemetry
    /// opt-in plus the Azure backend-token opt-in - and swaps the backend
    /// transport for a capturing handler so the wire request can be inspected
    /// offline. <c>ConfigureAll</c> is appended after the registration's own
    /// primary-handler action, so it wins for the single telemetry HTTP client
    /// without the test having to name it.
    /// </summary>
    private static ServiceProvider Host(
        Action<LatticeApiMcpTelemetryOptions> configure,
        FakeTokenCredential credential,
        CapturingHttpMessageHandler handler)
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(options =>
        {
            options.BackendAddress = new Uri(BackendBase);
            configure(options);
        });
        services.AddAzureTelemetryBackendToken(options => options.Credential = credential);
        services.ConfigureAll<HttpClientFactoryOptions>(options =>
            options.HttpMessageHandlerBuilderActions.Add(builder =>
            {
                // The product registration already built a primary handler by the
                // time this runs; dispose it before displacing it so the test
                // leaves no orphaned handler (or attached client certificate).
                (builder.PrimaryHandler as IDisposable)?.Dispose();
                builder.PrimaryHandler = handler;
            }));

        return services.BuildServiceProvider();
    }

    /// <summary>
    /// A credential that always mints the same sentinel with a never-reached
    /// expiry, so the provider's freshness check has a fixed answer and no
    /// assertion here depends on the wall clock.
    /// </summary>
    private static FakeTokenCredential MintingSentinel()
        => new(_ => new AccessToken(Sentinel, DateTimeOffset.MaxValue));

    // ---- The token reaches the backend, and only the backend ----

    [Test]
    public async Task The_minted_token_is_stamped_on_the_outbound_backend_request()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(credential.CallCount, Is.EqualTo(1), "The provider must mint exactly one token.");
            Assert.That(handler.LastAuthorization, Is.EqualTo($"Bearer {Sentinel}"));
        });
    }

    [Test]
    public async Task The_minted_token_only_ever_travels_to_the_configured_backend_address()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(handler.RequestCount, Is.EqualTo(1));
            Assert.That(
                handler.LastRequestUri!.GetLeftPart(UriPartial.Authority),
                Is.EqualTo(new Uri(BackendBase).GetLeftPart(UriPartial.Authority)),
                "A token-bearing request must never leave the configured backend authority.");
        });
    }

    [Test]
    public async Task The_minted_token_appears_on_no_header_other_than_authorization()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        var leaked = handler.LastHeaders
            .Where(h => !h.Key.Equals("Authorization", StringComparison.OrdinalIgnoreCase))
            .Where(h => h.Value.Contains(Sentinel, StringComparison.Ordinal))
            .Select(h => h.Key)
            .ToArray();

        Assert.That(leaked, Is.Empty, "The backend token must ride only the Authorization header.");
    }

    [Test]
    public async Task The_minted_token_is_not_encoded_into_the_request_uri()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        Assert.That(
            handler.LastRequestUri!.ToString(),
            Does.Not.Contain(Sentinel),
            "A query-string credential would be logged by every proxy on the path.");
    }

    // ---- The token never lands on the options instance the hoist made shared ----

    [Test]
    public async Task Minting_a_token_never_writes_it_onto_the_shared_telemetry_options()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        var binding = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;
        var neutral = provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(
                credential.CallCount,
                Is.EqualTo(1),
                "The assertion below is only meaningful if a token was actually minted.");
            Assert.That(
                neutral,
                Is.SameAs(binding),
                "Post-hoist both options types resolve to one object, so one leak would serve both readers.");
            Assert.That(
                binding.Credential,
                Is.Null,
                "The dynamic-bearer path must not cache the minted token onto the shared credential holder.");
        });
    }

    [Test]
    public async Task No_reachable_string_on_the_shared_options_carries_the_minted_token()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o =>
            {
                o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
                o.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
                o.AllowedMetrics.Add("up");
            },
            credential,
            handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;
        var carriers = ReadableStrings(options)
            .Where(value => value.Contains(Sentinel, StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                credential.CallCount,
                Is.EqualTo(1),
                "The assertion below is only meaningful if a token was actually minted.");
            Assert.That(carriers, Is.Empty, "No option a caller can read may carry the backend token.");
        });
    }

    // ---- The credential is minted only for the backend path that asked for it ----

    [TestCase(LatticeTelemetryBackendAuthMode.None)]
    [TestCase(LatticeTelemetryBackendAuthMode.Bearer)]
    [TestCase(LatticeTelemetryBackendAuthMode.Basic)]
    [TestCase(LatticeTelemetryBackendAuthMode.MutualTls)]
    public async Task A_static_auth_mode_never_mints_the_azure_credential(
        LatticeTelemetryBackendAuthMode mode)
    {
        // MutualTls is the only mode whose validator demands a certificate; the
        // others reject a null credential member, so each mode is given exactly the
        // material it requires and the provider is registered alongside regardless.
        using var certificate = mode == LatticeTelemetryBackendAuthMode.MutualTls
            ? SelfSignedCertificate()
            : null;

        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o =>
            {
                o.AuthMode = mode;
                o.Credential = new LatticeTelemetryBackendCredential
                {
                    BearerToken = StaticToken,
                    BasicUsername = "u",
                    BasicPassword = "p",
                    ClientCertificate = certificate,
                };
            },
            credential,
            handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                credential.CallCount,
                Is.Zero,
                "Registering the Azure provider must not mint a token for a mode that did not ask for one.");
            Assert.That(handler.LastAuthorization ?? string.Empty, Does.Not.Contain(Sentinel));
        });
    }

    /// <summary>
    /// An ephemeral self-signed certificate, so the mutual-TLS mode has the
    /// credential material its validator requires. Only its presence matters here;
    /// nothing asserts on its contents or validity window, so the test stays free
    /// of any wall-clock dependence.
    /// </summary>
    private static X509Certificate2 SelfSignedCertificate()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(
            "CN=orleans-lattice-telemetry-isolation-test", key, HashAlgorithmName.SHA256);
        return request.CreateSelfSigned(
            DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddYears(100));
    }

    [Test]
    public async Task A_static_bearer_mode_presents_the_host_token_not_the_azure_one()
    {
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        await using var provider = Host(
            o =>
            {
                o.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
                o.Credential = new LatticeTelemetryBackendCredential { BearerToken = StaticToken };
            },
            credential,
            handler);

        await provider.GetRequiredService<IPrometheusQueryClient>()
            .InstantQueryAsync("up", null, CancellationToken.None);

        Assert.That(handler.LastAuthorization, Is.EqualTo($"Bearer {StaticToken}"));
    }

    // ---- The credential is not reachable through the shipped surface ----

    [Test]
    public void The_container_exposes_exactly_one_backend_token_seam_and_no_other_route()
    {
        // Names the trust boundary rather than pretending it does not exist:
        // in-process host code that composed the container is trusted and can
        // resolve the seam by design. What this pins is that the opt-in adds
        // exactly ONE such route - so the reachable surface is the documented seam
        // and nothing more - and that the provider behind it is this package's.
        var credential = MintingSentinel();
        var handler = new CapturingHttpMessageHandler();
        using var provider = Host(
            o => o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer, credential, handler);

        var seams = provider.GetServices<ITelemetryBackendTokenProvider>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(seams, Has.Length.EqualTo(1));
            Assert.That(seams[0], Is.InstanceOf<AzureTelemetryBackendTokenProvider>());
            Assert.That(
                credential.CallCount,
                Is.Zero,
                "Merely resolving the seam must not mint a token; acquisition is lazy.");
        });
    }

    [Test]
    public void The_azure_token_provider_is_not_part_of_the_public_surface()
    {
        var exported = typeof(AzureTelemetryBackendTokenOptions).Assembly.GetExportedTypes();

        Assert.That(
            exported,
            Has.None.EqualTo(typeof(AzureTelemetryBackendTokenProvider)),
            "The provider must stay internal so ITelemetryBackendTokenProvider is the only path to it.");
    }

    [Test]
    public void The_bound_options_are_the_only_public_member_typed_to_hold_the_credential()
    {
        var holders = typeof(AzureTelemetryBackendTokenOptions).Assembly
            .GetExportedTypes()
            .SelectMany(type => type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                .Where(property => typeof(TokenCredential).IsAssignableFrom(property.PropertyType))
                .Select(property => $"{type.Name}.{property.Name}"))
            .ToArray();

        Assert.That(
            holders,
            Is.EqualTo(new[] { $"{nameof(AzureTelemetryBackendTokenOptions)}.{nameof(AzureTelemetryBackendTokenOptions.Credential)}" }),
            "A second public credential holder would widen the reachable surface of the Azure identity.");
    }

    [Test]
    public void The_telemetry_options_surface_cannot_hold_the_credential_or_the_token_seam()
    {
        var reachable = new[] { typeof(LatticeTelemetryOptions), typeof(LatticeApiMcpTelemetryOptions) }
            .SelectMany(type => type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            .Where(property =>
                typeof(TokenCredential).IsAssignableFrom(property.PropertyType)
                || typeof(ITelemetryBackendTokenProvider).IsAssignableFrom(property.PropertyType))
            .Select(property => $"{property.DeclaringType!.Name}.{property.Name}")
            .ToArray();

        Assert.That(
            reachable,
            Is.Empty,
            "The options object every telemetry caller can read must offer no route to the backend identity.");
    }

    /// <summary>
    /// Every string a caller holding the telemetry options could read: the scalar
    /// string properties, the string members of the credential holder, and the
    /// allow-list entries. Kept deliberately explicit rather than a reflective
    /// object walk, so the check fails loudly if the options surface grows a new
    /// member instead of silently skipping it.
    /// </summary>
    private static IEnumerable<string> ReadableStrings(LatticeTelemetryOptions options)
    {
        if (options.BackendAddress is { } address)
        {
            yield return address.ToString();
        }

        foreach (var allowed in options.AllowedMetrics)
        {
            yield return allowed;
        }

        if (options.Credential is not { } credential)
        {
            yield break;
        }

        yield return credential.BearerToken ?? string.Empty;
        yield return credential.BasicUsername ?? string.Empty;
        yield return credential.BasicPassword ?? string.Empty;
        yield return credential.ClientCertificate?.Thumbprint ?? string.Empty;
    }
}
