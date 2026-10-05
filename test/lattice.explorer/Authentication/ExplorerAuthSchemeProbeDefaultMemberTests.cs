using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// The probe contract's default member: a probe written before transport headers
/// existed implements only the two-argument overload, and the interface's own
/// three-argument default must forward to it so such a probe keeps working - it
/// simply cannot reach a header-gated endpoint.
/// </summary>
/// <remarks>
/// A default interface member is reached only through an implementor that does
/// not override it. <see cref="GrpcExplorerAuthSchemeProbe"/> implements both
/// overloads, and a substitute would override both, so neither reaches this
/// default; a hand-written probe that declares exactly one of them is the only
/// thing that does.
/// </remarks>
[TestFixture]
public sealed class ExplorerAuthSchemeProbeDefaultMemberTests
{
    [Test]
    public async Task The_header_aware_overload_forwards_to_the_older_one()
    {
        var probe = new HeaderBlindProbe(new ExplorerAuthSchemeAdvertisement
        {
            Schemes = [new ExplorerAuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra }],
        });

        var advertisement = await ((IExplorerAuthSchemeProbe)probe).ProbeAsync(
            "https://endpoint.example",
            allowUnencryptedHttp2: true,
            transportHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["x-azure-fdid"] = "origin" },
            CancellationToken.None);

        Assert.That(advertisement.Schemes.Single().SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
    }

    [Test]
    public async Task The_address_the_posture_and_the_token_all_survive_the_forward()
    {
        // The default's only possible defect is in what it forwards. Each argument
        // carries a distinct marker so a dropped or transposed one cannot pass.
        var probe = new HeaderBlindProbe(ExplorerAuthSchemeAdvertisement.Empty);
        using var cancellation = new CancellationTokenSource();

        await ((IExplorerAuthSchemeProbe)probe).ProbeAsync(
            "http://endpoint.example:5000",
            allowUnencryptedHttp2: true,
            transportHeaders: null,
            cancellation.Token);
        await ((IExplorerAuthSchemeProbe)probe).ProbeAsync(
            "https://other.example",
            allowUnencryptedHttp2: false,
            transportHeaders: null,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Addresses, Is.EqualTo(new[] { "http://endpoint.example:5000", "https://other.example" }));
            Assert.That(probe.Postures, Is.EqualTo(new[] { true, false }));
            Assert.That(probe.Tokens, Is.EqualTo(new[] { cancellation.Token, CancellationToken.None }));
        });
    }

    [Test]
    public void A_probe_that_implements_both_overloads_does_not_reach_the_default() =>
        Assert.That(
            typeof(GrpcExplorerAuthSchemeProbe)
                .GetMethods()
                .Count(method => method.Name == nameof(IExplorerAuthSchemeProbe.ProbeAsync)),
            Is.EqualTo(2),
            "the production probe overrides both, which is why the default needs its own implementor to be reached at all");

    /// <summary>A probe predating transport headers: it declares only the two-argument overload.</summary>
    private sealed class HeaderBlindProbe(ExplorerAuthSchemeAdvertisement advertisement) : IExplorerAuthSchemeProbe
    {
        public List<string> Addresses { get; } = [];

        public List<bool> Postures { get; } = [];

        public List<CancellationToken> Tokens { get; } = [];

        public Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
            string address,
            bool allowUnencryptedHttp2 = false,
            CancellationToken cancellationToken = default)
        {
            Addresses.Add(address);
            Postures.Add(allowUnencryptedHttp2);
            Tokens.Add(cancellationToken);
            return Task.FromResult(advertisement);
        }
    }
}
