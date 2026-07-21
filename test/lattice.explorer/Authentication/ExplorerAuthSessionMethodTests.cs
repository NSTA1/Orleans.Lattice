using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class ExplorerAuthSessionMethodTests
{
    private const string CustomScheme = "custom";

    private static (ExplorerAuthSession session, List<LatticeConnectionSettings> applied, InMemoryCredentialStore store, FakeSchemeProbe probe, FakeTokenAuthMethod custom)
        CreateSession(IReadOnlyDictionary<string, string>? transportHeaders = null)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var applied = new List<LatticeConnectionSettings>();
        connection
            .ConfigureAsync(Arg.Do<LatticeConnectionSettings>(applied.Add), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "https://cluster.internal:443",
            AllowUnencryptedHttp2 = false,
            TransportHeaders = transportHeaders,
        });

        var store = new InMemoryCredentialStore();
        var probe = new FakeSchemeProbe();
        var custom = new FakeTokenAuthMethod(CustomScheme);
        var session = new ExplorerAuthSession(
            explorerSession,
            store,
            seed: null,
            methods: new IExplorerAuthMethod[] { custom },
            probe: probe);
        return (session, applied, store, probe, custom);
    }

    [Test]
    public void AvailableSchemes_includesAutoAddedBasic_andRegisteredCustom()
    {
        var (session, _, _, _, _) = CreateSession();

        Assert.That(session.AvailableSchemes, Is.EquivalentTo(new[] { CustomScheme, "basic" }));
    }

    [Test]
    public async Task LoginWithMethodAsync_customTokenScheme_appliesBearer_andRunsCustomChallenge()
    {
        var (session, applied, _, _, custom) = CreateSession();

        await session.LoginWithMethodAsync(CustomScheme);

        Assert.Multiple(() =>
        {
            Assert.That(custom.ChallengeCount, Is.EqualTo(1), "the bespoke provider drove its own challenge");
            Assert.That(session.IsAuthenticated, Is.True);
            Assert.That(session.CurrentScheme, Is.EqualTo(CustomScheme));
            Assert.That(session.Username, Is.EqualTo("custom-user"));
            Assert.That(applied[^1].Authentication!.HasCredentialProvider, Is.True);
        });
    }

    [Test]
    public async Task LoginWithMethodAsync_tokenScheme_neverPersistsToStore()
    {
        var (session, _, store, _, _) = CreateSession();

        await session.LoginWithMethodAsync(CustomScheme);

        Assert.That(await store.GetAsync(), Is.Null, "token sign-ins are session-only and must never be persisted");
    }

    [Test]
    public async Task LoginWithMethodAsync_tokenScheme_clearsPreviouslyPersistedBasicCredential()
    {
        var (session, _, store, _, _) = CreateSession();
        await session.LoginAsync("alice", "Password1");
        Assert.That(await store.GetAsync(), Is.Not.Null);

        await session.LoginWithMethodAsync(CustomScheme);

        Assert.That(await store.GetAsync(), Is.Null, "switching to a token scheme clears the stale Basic credential");
    }

    [Test]
    public async Task LogoutAsync_afterTokenLogin_clearsStateAndReconfiguresAnonymously()
    {
        var (session, applied, _, _, _) = CreateSession();
        await session.LoginWithMethodAsync(CustomScheme);
        applied.Clear();

        await session.LogoutAsync();

        Assert.Multiple(() =>
        {
            Assert.That(session.IsAuthenticated, Is.False);
            Assert.That(session.CurrentScheme, Is.Null);
            Assert.That(applied, Has.Count.EqualTo(1));
            Assert.That(applied[0].Authentication, Is.Null);
        });
    }

    [Test]
    public async Task DiscoverAsync_returnsProbeAdvertisement_andSelectsMatchingMethod()
    {
        var (session, _, _, probe, custom) = CreateSession();
        probe.Result = new ExplorerAuthSchemeAdvertisement
        {
            Schemes = new[]
            {
                new ExplorerAuthSchemeDescriptor { SchemeId = CustomScheme, DisplayName = "Custom SSO" },
            },
        };

        var advertisement = await session.DiscoverAsync();
        var method = session.SelectMethodForAdvertisement(advertisement);

        Assert.Multiple(() =>
        {
            Assert.That(probe.ProbeCount, Is.EqualTo(1));
            Assert.That(advertisement.HasSchemes, Is.True);
            Assert.That(method, Is.SameAs(custom), "the advertised scheme selects its registered provider");
        });
    }

    [Test]
    public void SelectMethodForAdvertisement_unhandledScheme_returnsNull()
    {
        var (session, _, _, _, _) = CreateSession();
        var advertisement = new ExplorerAuthSchemeAdvertisement
        {
            Schemes = new[] { new ExplorerAuthSchemeDescriptor { SchemeId = "saml" } },
        };

        Assert.That(session.SelectMethodForAdvertisement(advertisement), Is.Null);
    }

    [Test]
    public void LoginWithMethodAsync_unknownScheme_throwsArgumentException()
    {
        var (session, _, _, _, _) = CreateSession();

        Assert.That(async () => await session.LoginWithMethodAsync("saml"), Throws.ArgumentException);
    }

    [Test]
    public async Task DiscoverAsync_forwardsAdvertisedParameters_toChallenge()
    {
        var (session, applied, _, probe, _) = CreateSession();
        probe.Result = new ExplorerAuthSchemeAdvertisement
        {
            Schemes = new[]
            {
                new ExplorerAuthSchemeDescriptor
                {
                    SchemeId = CustomScheme,
                    Parameters = new Dictionary<string, string>(StringComparer.Ordinal) { ["authority"] = "https://issuer" },
                },
            },
        };

        await session.DiscoverAsync();
        await session.LoginWithMethodAsync(CustomScheme);

        Assert.That(session.CurrentScheme, Is.EqualTo(CustomScheme));
        Assert.That(applied[^1].Authentication!.HasCredentialProvider, Is.True);
    }

    [Test]
    public async Task DiscoverAsync_passesConfiguredTransportHeaders_toProbe()
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["X-Azure-FDID"] = "ebe77622-4e04-4a58-a914-db561e750fe0",
        };
        var (session, _, _, probe, _) = CreateSession(headers);

        await session.DiscoverAsync();

        Assert.That(probe.LastTransportHeaders, Is.SameAs(headers),
            "the scheme probe must carry the same routing headers as the state client, or an origin-locked endpoint rejects the unauthenticated probe and discovery wrongly degrades to Basic");
    }

    [Test]
    public async Task DiscoverAsync_withoutTransportHeaders_passesNullToProbe()
    {
        var (session, _, _, probe, _) = CreateSession();

        await session.DiscoverAsync();

        Assert.That(probe.LastTransportHeaders, Is.Null);
    }
}
