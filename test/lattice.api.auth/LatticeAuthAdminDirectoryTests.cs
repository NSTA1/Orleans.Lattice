using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for the identity-directory and access-model half of the control
/// facade (issue #1248). Instantiates <see cref="LatticeAuthAdmin"/> directly
/// over a fake <see cref="ILatticeIdentityDirectory"/> and a stub access gate, so
/// no cluster is involved: it proves search / resolve delegate to the directory
/// and map to the wire DTOs, that a missing directory folds to an explicit
/// unavailable result rather than throwing, that the admin gate is enforced
/// fail-closed on every new operation, and that
/// <see cref="ILatticeAuthAdmin.GetAccessModelAsync"/> reports the silo's
/// authoritative access model.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminDirectoryTests
{
    private const string SearchTerm = "ali";

    private static LatticeAuthAdmin CreateAdmin(
        ILatticeIdentityDirectory identityDirectory,
        bool adminAllowed = true,
        bool nullGate = false,
        IEnumerable<ILatticeCredentialAuthenticator>? authenticators = null,
        SubjectGroupMergeMode groupMergeMode = SubjectGroupMergeMode.Union)
    {
        ILatticeAccessGate gate = nullGate ? new NullLatticeAccessGate() : new FakeAccessGate(adminAllowed);
        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions { GroupMergeMode = groupMergeMode });
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions());

        return new LatticeAuthAdmin(
            Substitute.For<ILatticeAuthorizationPolicyStore>(),
            Substitute.For<ILatticeMembershipDirectory>(),
            gate,
            new FakeMembershipContext(),
            identityDirectory,
            authenticators ?? new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() },
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor);
    }

    // ----- SearchDirectoryAsync -----

    [Test]
    public async Task SearchDirectoryAsync_delegates_to_the_directory_and_maps_principals()
    {
        var directory = new FakeIdentityDirectory
        {
            NextPage = new DirectorySearchPage(
                new[]
                {
                    new DirectoryPrincipal("u-1", "Alice", DirectoryPrincipalKind.User,
                        new Dictionary<string, string> { ["team"] = "ops" }),
                    new DirectoryPrincipal("g-1", "Admins", DirectoryPrincipalKind.Group),
                },
                ContinuationToken: "cursor-2"),
        };
        var admin = CreateAdmin(directory);

        var result = await admin.SearchDirectoryAsync(new DirectorySearchRequest
        {
            Term = SearchTerm,
            Kind = DirectoryPrincipalKind.User,
            PageSize = 25,
            ContinuationToken = "cursor-1",
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.True);
            Assert.That(result.ContinuationToken, Is.EqualTo("cursor-2"));
            Assert.That(result.Principals.Select(p => p.Id), Is.EqualTo(new[] { "u-1", "g-1" }));
            Assert.That(result.Principals[0].DisplayName, Is.EqualTo("Alice"));
            Assert.That(result.Principals[0].Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(result.Principals[0].Claims!["team"], Is.EqualTo("ops"));
            Assert.That(result.Principals[1].Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(result.Principals[1].Claims, Is.Null);
        });

        // The request shape reached the provider intact.
        Assert.Multiple(() =>
        {
            Assert.That(directory.LastQuery, Is.Not.Null);
            Assert.That(directory.LastQuery!.Value.Term, Is.EqualTo(SearchTerm));
            Assert.That(directory.LastQuery.Value.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(directory.LastQuery.Value.PageSize, Is.EqualTo(25));
            Assert.That(directory.LastQuery.Value.ContinuationToken, Is.EqualTo("cursor-1"));
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_folds_a_missing_directory_to_an_unavailable_result()
    {
        var admin = CreateAdmin(new NullIdentityDirectory());

        var result = await admin.SearchDirectoryAsync(new DirectorySearchRequest { Term = SearchTerm });

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.False);
            Assert.That(result.Principals, Is.Empty);
            Assert.That(result.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void SearchDirectoryAsync_is_denied_fail_closed_for_a_non_administrator()
    {
        var directory = new FakeIdentityDirectory();
        var admin = CreateAdmin(directory, adminAllowed: false);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => admin.SearchDirectoryAsync(new DirectorySearchRequest { Term = SearchTerm }));

        // The directory is never touched once authorization fails.
        Assert.That(directory.LastQuery, Is.Null);
    }

    [Test]
    public void SearchDirectoryAsync_rejects_a_null_request()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory());

        Assert.ThrowsAsync<ArgumentNullException>(() => admin.SearchDirectoryAsync(null!));
    }

    [Test]
    public async Task SearchDirectoryAsync_records_latency_and_a_hit_when_the_search_matches()
    {
        var directory = new FakeIdentityDirectory
        {
            NextPage = new DirectorySearchPage(
                new[] { new DirectoryPrincipal("u-1", "Alice", DirectoryPrincipalKind.User) },
                ContinuationToken: null),
        };
        var admin = CreateAdmin(directory);

        using var duration = new MeterCollector<double>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchDurationName);
        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchMissesName);

        await admin.SearchDirectoryAsync(new DirectorySearchRequest { Term = SearchTerm });

        Assert.Multiple(() =>
        {
            Assert.That(duration.Count, Is.EqualTo(1), "a directory-backed search records one latency sample");
            Assert.That(hits.Sum(), Is.EqualTo(1), "a search returning a principal counts a hit");
            Assert.That(misses.Sum(), Is.Zero);
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_records_a_miss_when_the_search_returns_nothing()
    {
        var directory = new FakeIdentityDirectory
        {
            NextPage = new DirectorySearchPage(Array.Empty<DirectoryPrincipal>(), ContinuationToken: null),
        };
        var admin = CreateAdmin(directory);

        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchMissesName);

        await admin.SearchDirectoryAsync(new DirectorySearchRequest { Term = SearchTerm });

        Assert.Multiple(() =>
        {
            Assert.That(misses.Sum(), Is.EqualTo(1), "an empty search counts a miss");
            Assert.That(hits.Sum(), Is.Zero);
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_records_nothing_when_no_directory_is_configured()
    {
        var admin = CreateAdmin(new NullIdentityDirectory());

        using var duration = new MeterCollector<double>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchDurationName);
        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchMissesName);

        var result = await admin.SearchDirectoryAsync(new DirectorySearchRequest { Term = SearchTerm });

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.False, "no configured directory folds to an unavailable result");
            Assert.That(duration.Count, Is.Zero, "the no-op provider is never called, so nothing is timed");
            Assert.That(hits.Sum(), Is.Zero);
            Assert.That(misses.Sum(), Is.Zero);
        });
    }

    // ----- ResolveDirectoryPrincipalAsync -----

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_delegates_and_maps_the_principal()
    {
        var directory = new FakeIdentityDirectory
        {
            NextPrincipal = new DirectoryPrincipal("u-7", "Bob", DirectoryPrincipalKind.User),
        };
        var admin = CreateAdmin(directory);

        var descriptor = await admin.ResolveDirectoryPrincipalAsync("u-7");

        Assert.Multiple(() =>
        {
            Assert.That(descriptor, Is.Not.Null);
            Assert.That(descriptor!.Id, Is.EqualTo("u-7"));
            Assert.That(descriptor.DisplayName, Is.EqualTo("Bob"));
            Assert.That(descriptor.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(directory.LastResolvedId, Is.EqualTo("u-7"));
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_returns_null_when_the_principal_is_unknown()
    {
        var directory = new FakeIdentityDirectory { NextPrincipal = null };
        var admin = CreateAdmin(directory);

        Assert.That(await admin.ResolveDirectoryPrincipalAsync("ghost"), Is.Null);
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_returns_null_when_no_directory_is_configured()
    {
        var admin = CreateAdmin(new NullIdentityDirectory());

        Assert.That(await admin.ResolveDirectoryPrincipalAsync("u-7"), Is.Null);
    }

    [Test]
    public void ResolveDirectoryPrincipalAsync_is_denied_fail_closed_for_a_non_administrator()
    {
        var directory = new FakeIdentityDirectory();
        var admin = CreateAdmin(directory, adminAllowed: false);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => admin.ResolveDirectoryPrincipalAsync("u-7"));

        Assert.That(directory.LastResolvedId, Is.Null);
    }

    [Test]
    public void ResolveDirectoryPrincipalAsync_rejects_a_null_or_empty_id()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory());

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => admin.ResolveDirectoryPrincipalAsync(null!));
            Assert.ThrowsAsync<ArgumentException>(() => admin.ResolveDirectoryPrincipalAsync(string.Empty));
        });
    }

    // ----- GetAccessModelAsync -----

    [Test]
    public async Task GetAccessModelAsync_requests_the_group_scoped_directory_guidance()
    {
        var directory = new FakeIdentityDirectory();
        var admin = CreateAdmin(directory);

        await admin.GetAccessModelAsync();

        Assert.That(directory.LastDescribeKind, Is.EqualTo(DirectoryPrincipalKind.Group));
    }

    [Test]
    public async Task GetAccessModelAsync_reports_an_available_directory_with_provider_and_explanation()
    {
        var directory = new FakeIdentityDirectory();
        var admin = CreateAdmin(directory);

        var model = await admin.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(model.DirectoryAvailable, Is.True);
            Assert.That(model.DirectoryProviderId, Is.EqualTo(FakeIdentityDirectory.Provider));
            Assert.That(model.DirectoryExplanation, Is.EqualTo(FakeIdentityDirectory.Guidance));
            Assert.That(model.RulesEnforced, Is.True);
        });
    }

    [Test]
    public async Task GetAccessModelAsync_reports_an_unavailable_null_directory()
    {
        var directory = new NullIdentityDirectory();
        var admin = CreateAdmin(directory);

        var model = await admin.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(model.DirectoryAvailable, Is.False);
            Assert.That(model.DirectoryProviderId, Is.EqualTo(NullIdentityDirectory.NullProviderId));
            Assert.That(model.DirectoryExplanation, Is.EqualTo(directory.DescribeEntry(DirectoryPrincipalKind.Group)));
        });
    }

    [Test]
    public async Task GetAccessModelAsync_reports_rules_unenforced_under_the_null_gate()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory(), nullGate: true);

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.RulesEnforced, Is.False);
    }

    [Test]
    public async Task GetAccessModelAsync_reports_local_membership_effective_under_the_union_merge_mode()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory(), groupMergeMode: SubjectGroupMergeMode.Union);

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.LocalMembershipEffective, Is.True);
    }

    [Test]
    public async Task GetAccessModelAsync_reports_local_membership_effective_under_the_directory_only_merge_mode()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory(), groupMergeMode: SubjectGroupMergeMode.DirectoryOnly);

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.LocalMembershipEffective, Is.True);
    }

    [Test]
    public async Task GetAccessModelAsync_reports_local_membership_inert_under_the_token_only_merge_mode()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory(), groupMergeMode: SubjectGroupMergeMode.TokenOnly);

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.LocalMembershipEffective, Is.False);
    }

    [Test]
    public async Task GetAccessModelAsync_reports_claims_mode_when_a_real_authenticator_is_registered()
    {
        var admin = CreateAdmin(
            new FakeIdentityDirectory(),
            authenticators: new ILatticeCredentialAuthenticator[]
            {
                new AnonymousCredentialAuthenticator(),
                new FakeCredentialAuthenticator(),
            });

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Claims));
    }

    [Test]
    public async Task GetAccessModelAsync_reports_anonymous_mode_when_only_the_anonymous_fallback_is_registered()
    {
        var admin = CreateAdmin(
            new FakeIdentityDirectory(),
            authenticators: new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() });

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Anonymous));
    }

    [Test]
    public async Task GetAccessModelAsync_reports_unknown_mode_when_no_authenticator_is_registered()
    {
        var admin = CreateAdmin(
            new FakeIdentityDirectory(),
            authenticators: Array.Empty<ILatticeCredentialAuthenticator>());

        var model = await admin.GetAccessModelAsync();

        Assert.That(model.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Unknown));
    }

    [Test]
    public void GetAccessModelAsync_is_denied_fail_closed_for_a_non_administrator()
    {
        var admin = CreateAdmin(new FakeIdentityDirectory(), adminAllowed: false);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => admin.GetAccessModelAsync());
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        ILatticeAccessGate gate = new NullLatticeAccessGate();
        var membership = new FakeMembershipContext();
        var identity = new FakeIdentityDirectory();
        var authenticators = new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() };
        var apiOptions = Options.Create(new LatticeApiAuthOptions());
        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAuthAdmin(
                store, directory, gate, membership, null!, authenticators, apiOptions, authMonitor, membershipMonitor, identityMonitor));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAuthAdmin(
                store, directory, gate, membership, identity, null!, apiOptions, authMonitor, membershipMonitor, identityMonitor));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAuthAdmin(
                store, directory, gate, membership, identity, authenticators, apiOptions, authMonitor, null!, identityMonitor));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAuthAdmin(
                store, directory, gate, membership, identity, authenticators, apiOptions, authMonitor, membershipMonitor, null!));
        });
    }

    private sealed class FakeAccessGate(bool allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by fake gate"));
    }

    private sealed class FakeMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }

    private sealed class FakeIdentityDirectory : ILatticeIdentityDirectory
    {
        public const string Provider = "fake";
        public const string Guidance = "Enter a fake-directory principal id.";

        public string ProviderId => Provider;

        public DirectoryPrincipalKind? LastDescribeKind { get; private set; }

        public string DescribeEntry(DirectoryPrincipalKind? kind)
        {
            LastDescribeKind = kind;
            return Guidance;
        }

        public DirectorySearchQuery? LastQuery { get; private set; }

        public string? LastResolvedId { get; private set; }

        public DirectorySearchPage NextPage { get; init; } = DirectorySearchPage.Empty;

        public DirectoryPrincipal? NextPrincipal { get; init; }

        public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)
        {
            LastQuery = query;
            return Task.FromResult(NextPage);
        }

        public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
        {
            LastResolvedId = principalId;
            return Task.FromResult(NextPrincipal);
        }
    }

    private sealed class FakeCredentialAuthenticator : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) => true;

        public ValueTask<LatticePrincipal?> AuthenticateAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken = default) =>
            new((LatticePrincipal?)null);
    }
}
