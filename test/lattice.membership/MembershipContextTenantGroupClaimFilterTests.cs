using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the tenant group claim filter wiring in
/// <see cref="MembershipContext"/> (epic #4154, D2 and D11). Active: a
/// claim-asserted or claim-projected <c>t/...</c> group is stripped before group
/// expansion, while directory-derived tenant groups and the tenant groups an
/// asserted cluster group expands into are kept. Inactive: resolution is
/// unchanged, the filter is never invoked, and the hook is one <c>bool</c> read
/// that hands the subject's group set back uncopied and allocates nothing.
/// </summary>
[TestFixture]
public sealed class MembershipContextTenantGroupClaimFilterTests
{
    private const string Scheme = "issuer-a";

    private static IOptionsMonitor<LatticeMembershipOptions> OptionsMonitor(LatticeMembershipOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options ?? new LatticeMembershipOptions());
        return monitor;
    }

    private static MembershipContext CreateContext(
        ILatticeMembershipDirectory directory,
        ITenantGroupClaimFilter claimFilter,
        IReadOnlyCollection<string>? assertedGroups = null,
        LatticeMembershipOptions? options = null,
        IReadOnlyDictionary<string, string>? claims = null)
    {
        var monitor = OptionsMonitor(options);
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == Scheme,
            _ => new LatticePrincipal("alice", Scheme, claims, assertedGroups));
        return new MembershipContext(
            new[] { authenticator },
            new DefaultLatticeSubjectMapper(monitor),
            directory,
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor,
            claimFilter);
    }

    private static async Task<LatticeSubject> ResolveAsync(MembershipContext context)
    {
        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            return await context.ResolveCurrentAsync();
        }
    }

    [Test]
    public async Task Active_filter_strips_a_token_asserted_tenant_group_before_expansion()
    {
        var directory = new ExpansionDirectory(
            directoryGroups: ["t/acme/members"],
            parents: new Dictionary<string, string[]> { ["entra-engineering"] = ["t/acme/engineers"] });
        var context = CreateContext(
            directory,
            new TenantGroupClaimFilter(static () => true),
            assertedGroups: ["t/acme/admins", "entra-engineering"]);

        var subject = await ResolveAsync(context);

        Assert.Multiple(() =>
        {
            Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "t/acme/members", "entra-engineering", "t/acme/engineers" }));
            Assert.That(directory.ExpandSeeds, Does.Not.Contain("t/acme/admins"), "the asserted tenant group must be stripped before expansion");
        });
    }

    [Test]
    public async Task Active_filter_strips_malformed_and_default_tenant_ids_too()
    {
        var directory = new ExpansionDirectory(directoryGroups: [], parents: new Dictionary<string, string[]>());
        var context = CreateContext(
            directory,
            new TenantGroupClaimFilter(static () => true),
            assertedGroups: ["t/default/admins", "t/BAD", "cluster-readers"]);

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "cluster-readers" }));
    }

    [Test]
    public async Task Active_filter_strips_a_claim_projected_tenant_group()
    {
        var directory = new ExpansionDirectory(directoryGroups: [], parents: new Dictionary<string, string[]>());
        var options = new LatticeMembershipOptions
        {
            ClaimToGroups = static claims => claims.TryGetValue("role", out var role) ? new[] { role, "projected" } : [],
        };
        var context = CreateContext(
            directory,
            new TenantGroupClaimFilter(static () => true),
            options: options,
            claims: new Dictionary<string, string> { ["role"] = "t/acme/admins" });

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "projected" }));
    }

    [Test]
    public async Task Active_filter_keeps_a_tenant_group_the_directory_records()
    {
        var directory = new ExpansionDirectory(directoryGroups: ["t/acme/members"], parents: new Dictionary<string, string[]>());
        var context = CreateContext(
            directory,
            new TenantGroupClaimFilter(static () => true),
            assertedGroups: ["t/acme/members"]);

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "t/acme/members" }));
    }

    [Test]
    public async Task Active_filter_in_token_only_mode_strips_every_asserted_tenant_group()
    {
        var directory = new ExpansionDirectory(directoryGroups: ["t/acme/members"], parents: new Dictionary<string, string[]>());
        var context = CreateContext(
            directory,
            new TenantGroupClaimFilter(static () => true),
            assertedGroups: ["t/acme/members", "cluster-readers"],
            options: new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.TokenOnly });

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "cluster-readers" }));
    }

    [Test]
    public async Task Inactive_filter_leaves_resolution_unchanged_and_is_never_invoked()
    {
        var spy = new InactiveSpyFilter();
        var directory = new ExpansionDirectory(
            directoryGroups: ["t/acme/members"],
            parents: new Dictionary<string, string[]> { ["entra-engineering"] = ["t/acme/engineers"] });
        var context = CreateContext(directory, spy, assertedGroups: ["t/acme/admins", "entra-engineering"]);

        var subject = await ResolveAsync(context);

        Assert.Multiple(() =>
        {
            Assert.That(
                subject.GroupIds,
                Is.EquivalentTo(new[] { "t/acme/members", "t/acme/admins", "entra-engineering", "t/acme/engineers" }));
            Assert.That(spy.IsActiveReads, Is.EqualTo(1), "the inactive path is one IsActive read per cold resolution");
        });
    }

    [Test]
    public async Task Inactive_filter_resolves_identically_to_the_pre_seam_constructor()
    {
        var parents = new Dictionary<string, string[]> { ["entra-engineering"] = ["t/acme/engineers"] };
        var asserted = new[] { "t/acme/admins", "entra-engineering" };
        var monitor = OptionsMonitor();
        var authenticator = new FakeAuthenticator(c => c.Scheme == Scheme, _ => new LatticePrincipal("alice", Scheme, null, asserted));
        var legacy = new MembershipContext(
            new[] { authenticator },
            new DefaultLatticeSubjectMapper(monitor),
            new ExpansionDirectory(["t/acme/members"], parents),
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor);
        var seamed = CreateContext(new ExpansionDirectory(["t/acme/members"], parents), new NullTenantGroupClaimFilter(), asserted);

        var expected = await ResolveAsync(legacy);
        var actual = await ResolveAsync(seamed);

        Assert.Multiple(() =>
        {
            Assert.That(legacy.ClaimFilter, Is.TypeOf<NullTenantGroupClaimFilter>());
            Assert.That(actual.SubjectId, Is.EqualTo(expected.SubjectId));
            Assert.That(actual.GroupIds, Is.EquivalentTo(expected.GroupIds));
        });
    }

    [Test]
    public void ApplyTenantGroupClaimFilter_inactive_returns_the_subject_unchanged()
    {
        var context = CreateContext(new ExpansionDirectory([], new Dictionary<string, string[]>()), new NullTenantGroupClaimFilter());
        var subject = new LatticeSubject("alice", new[] { "t/acme/admins" }, null);

        var result = context.ApplyTenantGroupClaimFilter(subject, Array.Empty<string>());

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(subject));
            Assert.That(result.GroupIds, Is.SameAs(subject.GroupIds), "the group set is handed back, not copied");
        });
    }

    [Test]
    public void ApplyTenantGroupClaimFilter_inactive_allocates_nothing()
    {
        var context = CreateContext(new ExpansionDirectory([], new Dictionary<string, string[]>()), new NullTenantGroupClaimFilter());
        var subject = new LatticeSubject("alice", new[] { "t/acme/admins", "cluster-readers" }, null);
        var directoryGroups = new[] { "cluster-readers" };

        var growth = AllocationProbe.Growth(
            prepare: _ => context,
            measure: (ctx, size) =>
            {
                for (var i = 0; i < size; i++)
                {
                    AllocationProbe.ScalarSink += ctx.ApplyTenantGroupClaimFilter(subject, directoryGroups).GroupIds.Count;
                }
            },
            smallSize: 16,
            largeSize: 1024);

        Assert.That(growth, Is.Zero);
    }

    [Test]
    public void ApplyTenantGroupClaimFilter_active_with_nothing_to_strip_returns_the_subject_unchanged()
    {
        var context = CreateContext(new ExpansionDirectory([], new Dictionary<string, string[]>()), new TenantGroupClaimFilter(static () => true));
        var subject = new LatticeSubject("alice", new[] { "t/acme/members", "cluster-readers" }, null);

        var result = context.ApplyTenantGroupClaimFilter(subject, new[] { "t/acme/members" });

        Assert.That(result.GroupIds, Is.SameAs(subject.GroupIds));
    }

    [Test]
    public void Constructor_null_claim_filter_throws()
    {
        var monitor = OptionsMonitor();

        Assert.That(
            () => new MembershipContext(
                Array.Empty<ILatticeCredentialAuthenticator>(),
                new DefaultLatticeSubjectMapper(monitor),
                new ExpansionDirectory([], new Dictionary<string, string[]>()),
                new MembershipResolutionCache(TimeProvider.System, monitor),
                monitor,
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeMembership_context_consults_the_registered_filter()
    {
        var (provider, _) = BuildProvider(replaceWith: null);
        using (provider)
        {
            var context = (MembershipContext)provider.GetRequiredService<ILatticeMembershipContext>();
            Assert.That(context.ClaimFilter, Is.TypeOf<NullTenantGroupClaimFilter>());
        }

        var active = new TenantGroupClaimFilter(static () => true);
        var (replaced, _) = BuildProvider(replaceWith: active);
        using (replaced)
        {
            var context = (MembershipContext)replaced.GetRequiredService<ILatticeMembershipContext>();
            Assert.That(context.ClaimFilter, Is.SameAs(active));
        }
    }

    private static (ServiceProvider Provider, IServiceCollection Services) BuildProvider(ITenantGroupClaimFilter? replaceWith)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeMembership();

        // The membership initializer resolves the view factory at construction;
        // stand it in so resolving the context needs no core view wiring.
        services.AddSingleton(Substitute.For<ILatticeViewFactory>());
        if (replaceWith is not null)
        {
            services.Replace(ServiceDescriptor.Singleton(replaceWith));
        }

        return (services.BuildServiceProvider(), services);
    }

    /// <summary>
    /// A directory fake that returns fixed directory groups and expands seeds
    /// through a fixed parent map, recording the seeds it was asked to expand.
    /// </summary>
    private sealed class ExpansionDirectory(IReadOnlyCollection<string> directoryGroups, IReadOnlyDictionary<string, string[]> parents)
        : ILatticeMembershipDirectory
    {
        public List<string> ExpandSeeds { get; } = new();

        public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<string>>(new HashSet<string>(directoryGroups, StringComparer.Ordinal));

        public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default)
        {
            ExpandSeeds.AddRange(seedGroups);
            var closure = new HashSet<string>(seedGroups, StringComparer.Ordinal);
            foreach (var seed in seedGroups)
            {
                if (parents.TryGetValue(seed, out var groupParents))
                {
                    closure.UnionWith(groupParents);
                }
            }

            return Task.FromResult<IReadOnlyCollection<string>>(closure);
        }

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

    /// <summary>An inactive filter that counts IsActive reads and fails if it is ever asked to filter.</summary>
    private sealed class InactiveSpyFilter : ITenantGroupClaimFilter
    {
        public int IsActiveReads { get; private set; }

        public bool IsActive
        {
            get
            {
                IsActiveReads++;
                return false;
            }
        }

        public void Filter(ICollection<string> assertedGroups) =>
            Assert.Fail("an inactive filter must never be asked to filter");
    }
}
