using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for the membership resolution cache across flips of
/// <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/> (epic
/// #4154, D2). Once tenancy is registered an identity provider's asserted
/// <c>t/</c> group is stripped whatever the flag, so no subject is ever cached
/// carrying one, and a flag change in either direction still flushes the cache.
/// Wired through <c>AddLatticeMembership</c> and <c>AddLatticeTenancy</c>, so the
/// test exercises the production registration rather than a hand-made subscription.
/// </summary>
[TestFixture]
public sealed class DelegatedAccessMembershipCacheTests
{
    private const string Scheme = "issuer-a";
    private const string AssertedTenantGroup = "t/acme/admins";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public async Task With_the_flag_off_an_asserted_tenant_group_is_stripped_before_it_is_cached_and_a_flip_flushes_the_cache()
    {
        using var provider = BuildProvider();
        var flag = provider.GetRequiredService<DelegatedTenantAccessFlag>();
        var cache = provider.GetRequiredService<MembershipResolutionCache>();
        var membership = provider.GetRequiredService<ILatticeMembershipContext>();
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["owner", AssertedTenantGroup]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);
        var engine = new LatticeTenantPolicyEngine(maintainer);

        var cachedWhileOff = await ResolveAsync(membership);
        Assert.Multiple(() =>
        {
            Assert.That(cachedWhileOff.GroupIds, Does.Not.Contain(AssertedTenantGroup), "off, the asserted tenant group is stripped all the same");
            Assert.That(cachedWhileOff.GroupIds, Does.Contain("entra-sales"), "asserted cluster groups are kept");
            Assert.That(cache.Count, Is.EqualTo(1), "precondition: the subject is cached");
        });

        flag.Set(true);
        await maintainer.BackgroundRebuild;

        Assert.That(cache.Count, Is.Zero, "the flip flushes the membership resolution cache");
        var resolvedWhileOn = await ResolveAsync(membership);
        var decision = engine.ValidateActiveTenantAs(resolvedWhileOn.SubjectId, resolvedWhileOn.GroupIds, Acme);
        Assert.Multiple(() =>
        {
            Assert.That(resolvedWhileOn.GroupIds, Does.Not.Contain(AssertedTenantGroup), "the next resolution strips the asserted tenant group");
            Assert.That(decision.Allowed, Is.False, "an IdP-asserted tenant group never makes the subject a tenant admin");
        });
    }

    [Test]
    public async Task Turning_the_flag_off_flushes_the_cache_and_keeps_stripping()
    {
        using var provider = BuildProvider(enabled: true);
        var flag = provider.GetRequiredService<DelegatedTenantAccessFlag>();
        var cache = provider.GetRequiredService<MembershipResolutionCache>();
        var membership = provider.GetRequiredService<ILatticeMembershipContext>();

        var whileOn = await ResolveAsync(membership);
        Assert.That(whileOn.GroupIds, Does.Not.Contain(AssertedTenantGroup), "precondition: on, the asserted group is stripped");
        Assert.That(cache.Count, Is.EqualTo(1));

        flag.Set(false);

        Assert.That(cache.Count, Is.Zero);
        Assert.That(
            (await ResolveAsync(membership)).GroupIds,
            Does.Not.Contain(AssertedTenantGroup),
            "off, the t/ namespace stays reserved: an operator rule or app role naming the group must not match a token");
    }

    [Test]
    public async Task A_resolution_in_flight_across_a_flush_is_not_cached()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var cache = new MembershipResolutionCache(TimeProvider.System, monitor);
        var key = MembershipCacheKey.For(new LatticeCredential("tok", Scheme));

        var subject = await cache.ResolveAsync(
            key,
            _ =>
            {
                // The flag flips while this resolution, made under the old verdict, is still running.
                cache.Clear();
                return new ValueTask<ResolvedSubject>(new ResolvedSubject(new LatticeSubject("alice", [AssertedTenantGroup]), null));
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(subject.SubjectId, Is.EqualTo("alice"), "the caller still gets its answer");
            Assert.That(cache.Count, Is.Zero, "but a result resolved before the flush is never cached after it");
        });

        await cache.ResolveAsync(
            key,
            _ => new ValueTask<ResolvedSubject>(new ResolvedSubject(new LatticeSubject("alice"), null)),
            CancellationToken.None);
        Assert.That(cache.Count, Is.EqualTo(1), "a resolution that starts after the flush caches normally");
    }

    private static async Task<LatticeSubject> ResolveAsync(ILatticeMembershipContext membership)
    {
        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            return await membership.ResolveCurrentAsync();
        }
    }

    private static ServiceProvider BuildProvider(bool enabled = false)
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<IGrainFactory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        builder.AddLatticeMembership();
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeMembershipDirectory>(new EmptyDirectory()));
        builder.Services.AddSingleton(Substitute.For<ILatticeViewFactory>());
        builder.Services.AddSingleton<ILatticeCredentialAuthenticator>(new AssertingAuthenticator());
        builder.AddLatticeTenancy(o => o.DelegatedAccessAdministrationEnabled = enabled);
        return builder.Services.BuildServiceProvider();
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class CovSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }

    /// <summary>Authenticates the test scheme as <c>alice</c>, asserting a tenant group and a cluster group.</summary>
    private sealed class AssertingAuthenticator : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) => credential.Scheme == Scheme;

        public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default) =>
            new(new LatticePrincipal("alice", Scheme, null, [AssertedTenantGroup, "entra-sales"]));
    }

    /// <summary>A directory that records no memberships and expands seeds to themselves.</summary>
    private sealed class EmptyDirectory : ILatticeMembershipDirectory
    {
        public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<string>>(Array.Empty<string>());

        public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<string>>(new HashSet<string>(seedGroups, StringComparer.Ordinal));

        public Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
