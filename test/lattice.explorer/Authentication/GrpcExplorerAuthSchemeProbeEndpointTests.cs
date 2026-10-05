using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Tests.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// The production auth-scheme probe against a real endpoint: what an advertising
/// endpoint yields, what an endpoint that advertises nothing yields, what an
/// endpoint that is not there yields, and that the transport headers a fronting
/// proxy requires actually reach the wire.
/// </summary>
/// <remarks>
/// <para>
/// The probe builds its own channel through
/// <see cref="Orleans.Lattice.Explorer.Core.Connection.LatticeGrpcChannelFactory"/>,
/// so no handler can be swapped into it and a real listening endpoint is the only
/// way to drive the body at all. Everything below the argument guards - the
/// channel, the invoker, the RPC and the mapping - is reachable only this way.
/// </para>
/// <para>
/// The two refusals this type can produce are deliberately indistinguishable to a
/// caller: an endpoint that advertises nothing and an endpoint that is not there
/// both answer <see cref="ExplorerAuthSchemeAdvertisement.Empty"/>, because in
/// both cases the sign-in must fall back to Basic. They are separate tests
/// because they are separate arms - a <c>Map</c> that returned
/// <see cref="ExplorerAuthSchemeAdvertisement.Empty"/> unconditionally would pass
/// one of them.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class GrpcExplorerAuthSchemeProbeEndpointTests
{
    [Test]
    public async Task An_advertising_endpoint_yields_every_scheme_in_the_servers_order()
    {
        // A distinct marker on each element: two schemes whose ids, names and
        // parameters all differ, so a mapping that dropped, reordered or
        // cross-wired a field cannot pass.
        await using var server = await StartAsync(
            new AuthSchemeDescriptor
            {
                SchemeId = ExplorerAuthSchemes.Entra,
                DisplayName = "Microsoft Entra ID",
                Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["authority"] = "https://login.example/tenant",
                    ["clientId"] = "client-1",
                },
            },
            new AuthSchemeDescriptor
            {
                SchemeId = "oidc",
                DisplayName = "Corporate sign-in",
                Parameters = new Dictionary<string, string>(StringComparer.Ordinal) { ["audience"] = "api://lattice" },
            });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var advertisement = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true);

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.HasSchemes, Is.True);
            Assert.That(
                advertisement.Schemes.Select(scheme => scheme.SchemeId),
                Is.EqualTo(new[] { ExplorerAuthSchemes.Entra, "oidc" }),
                "the server's preference order is the whole point of the list");
            Assert.That(
                advertisement.Schemes.Select(scheme => scheme.DisplayName),
                Is.EqualTo(new[] { "Microsoft Entra ID", "Corporate sign-in" }));
            Assert.That(advertisement.Schemes[0].Parameters, Is.EqualTo(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["authority"] = "https://login.example/tenant",
                ["clientId"] = "client-1",
            }));
            Assert.That(advertisement.Schemes[1].Parameters, Is.EqualTo(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["audience"] = "api://lattice",
            }));
        });
    }

    [Test]
    public async Task A_single_scheme_endpoint_yields_exactly_one_descriptor()
    {
        // The mapping allocates an array of the advertised length and fills it by
        // index; one element is the arity a fencepost error would get wrong.
        await using var server = await StartAsync(new AuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Basic });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var advertisement = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true);

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.Schemes.Select(scheme => scheme.SchemeId), Is.EqualTo(new[] { ExplorerAuthSchemes.Basic }));
            Assert.That(advertisement.Schemes.Single().DisplayName, Is.Empty, "the server left it unset and the probe must not invent one");
            Assert.That(advertisement.Schemes.Single().Parameters, Is.Empty);
        });
    }

    [Test]
    public async Task An_endpoint_that_advertises_nothing_yields_the_empty_advertisement()
    {
        await using var server = await StartAsync();

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var advertisement = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true);

        Assert.Multiple(() =>
        {
            Assert.That(advertisement, Is.SameAs(ExplorerAuthSchemeAdvertisement.Empty));
            Assert.That(advertisement.HasSchemes, Is.False, "so the sign-in falls back to Basic");
        });
    }

    [Test]
    public async Task An_endpoint_that_is_not_there_yields_the_empty_advertisement_rather_than_failing()
    {
        // The other arm that produces Empty, and the reason the two need separate
        // tests: this one never reaches Map at all.
        using var probe = new GrpcExplorerAuthSchemeProbe();

        var advertisement = await probe.ProbeAsync(StateApiH2cServer.UnreachableAddress, allowUnencryptedHttp2: true);

        Assert.That(advertisement, Is.SameAs(ExplorerAuthSchemeAdvertisement.Empty));
    }

    [Test]
    public async Task The_probe_sends_the_transport_headers_a_fronting_proxy_requires()
    {
        await using var server = await StartAsync(new AuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var advertisement = await probe.ProbeAsync(
            server.Address,
            allowUnencryptedHttp2: true,
            transportHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["x-azure-fdid"] = "origin-token" },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                server.RequestHeaders.Select(headers => headers.GetValueOrDefault("x-azure-fdid")),
                Has.Some.EqualTo("origin-token"),
                "without the header a proxy-guarded endpoint rejects the probe and the sign-in wrongly falls back to Basic");
            Assert.That(advertisement.HasSchemes, Is.True, "the headered probe must still be answered");
        });
    }

    [Test]
    public async Task A_probe_with_no_transport_headers_sends_none()
    {
        // The contrast that keeps the test above honest: the header is the
        // caller's, not something the channel adds to every probe.
        await using var server = await StartAsync(new AuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true, transportHeaders: null, CancellationToken.None);

        Assert.That(
            server.RequestHeaders.Select(headers => headers.GetValueOrDefault("x-azure-fdid")),
            Has.None.EqualTo("origin-token"));
    }

    [Test]
    public async Task The_short_overload_probes_the_same_endpoint_with_no_headers()
    {
        // The two-argument overload is a one-line delegation, so the only thing it
        // can get wrong is which call it forwards to; driving it against a real
        // endpoint is what distinguishes "forwards" from "returns Empty".
        await using var server = await StartAsync(new AuthSchemeDescriptor { SchemeId = "oidc", DisplayName = "Corporate sign-in" });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var advertisement = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true);

        Assert.That(advertisement.Schemes.Single().DisplayName, Is.EqualTo("Corporate sign-in"));
    }

    [Test]
    public async Task One_probe_serves_more_than_one_endpoint()
    {
        // The probe owns a serializer provider for its whole lifetime and builds a
        // fresh short-lived channel per call, so a second call against a different
        // endpoint must neither reuse the first endpoint nor find a disposed provider.
        await using var entra = await StartAsync(new AuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra });
        await using var silent = await StartAsync();

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var first = await probe.ProbeAsync(entra.Address, allowUnencryptedHttp2: true);
        var second = await probe.ProbeAsync(silent.Address, allowUnencryptedHttp2: true);
        var third = await probe.ProbeAsync(entra.Address, allowUnencryptedHttp2: true);

        Assert.Multiple(() =>
        {
            Assert.That(first.Schemes.Single().SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
            Assert.That(second.HasSchemes, Is.False);
            Assert.That(third.Schemes.Single().SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra), "the first endpoint is still probeable after a second one answered");
        });
    }

    [Test]
    public async Task The_credential_free_probe_reaches_a_plaintext_endpoint_whatever_the_transport_posture()
    {
        // allowUnencryptedHttp2 lifts gRPC's insecure-channel safeguard, and that
        // safeguard only ever applies to a channel carrying a credential. The
        // probe deliberately carries none - it must succeed before the user has
        // one - so the posture cannot gate it, and a probe that refused with the
        // flag off would wrongly report a live endpoint as advertising nothing.
        await using var server = await StartAsync(new AuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra });

        using var probe = new GrpcExplorerAuthSchemeProbe();
        var guarded = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: false);
        var optedIn = await probe.ProbeAsync(server.Address, allowUnencryptedHttp2: true);

        Assert.Multiple(() =>
        {
            Assert.That(guarded.Schemes.Single().SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
            Assert.That(optedIn.Schemes.Single().SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
        });
    }

    private static Task<StateApiH2cServer> StartAsync(params AuthSchemeDescriptor[] advertised) =>
        StateApiH2cServer.StartAsync(
            Substitute.For<ILatticeStateQuery>(),
            advertisedAuthSchemes: advertised);
}
