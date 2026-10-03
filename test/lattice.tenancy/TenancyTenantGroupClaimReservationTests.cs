using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// The reserved <c>t/</c> group namespace holds whatever the delegated tenant access
/// administration flag says (epic #4154, D2). Operator rules and app role bindings
/// may name a tenant group and are honoured whatever the flag, so once tenancy is
/// registered membership's tenant group claim filter is always active: a token
/// asserting <c>t/{tenant}/{name}</c> never carries that group, so it never matches
/// a rule or a role binding naming it, even with the flag off.
/// </summary>
[TestFixture]
public sealed class TenancyTenantGroupClaimReservationTests
{
    private const string Scheme = "issuer-b";
    private const string Finance = "t/acme/finance";
    private const string Orders = "t/acme/orders";

    [Test]
    public void AddLatticeTenancy_claim_filter_is_active_whatever_the_flag()
    {
        var builder = NewBuilder();
        builder.Services.AddSingleton(Substitute.For<ITenantGroupClaimFilter>());

        builder.AddLatticeTenancy();

        Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantGroupClaimFilter)), Is.EqualTo(1));

        using var provider = builder.Services.BuildServiceProvider();
        var filter = provider.GetRequiredService<ITenantGroupClaimFilter>();
        var flag = provider.GetRequiredService<DelegatedTenantAccessFlag>();
        Assert.Multiple(() =>
        {
            Assert.That(filter.GetType().Name, Is.EqualTo("TenantGroupClaimFilter"), "membership's active filter, not a tenancy copy");
            Assert.That(flag.IsEnabled, Is.False, "the flag is off by default");
            Assert.That(filter.IsActive, Is.True, "the t/ namespace is reserved with the flag off");
        });

        flag.Set(true);
        Assert.That(filter.IsActive, Is.True);

        flag.Set(false);
        Assert.That(filter.IsActive, Is.True, "turning the flag back off does not release the namespace");
    }

    [Test]
    public async Task With_the_flag_off_an_asserted_tenant_group_is_stripped_and_matches_no_operator_rule()
    {
        using var provider = BuildTenancyProvider(enabled: false);
        Assert.That(provider.GetRequiredService<DelegatedTenantAccessFlag>().IsEnabled, Is.False);
        var context = CreateContext(provider.GetRequiredService<ITenantGroupClaimFilter>(), Finance, "entra-sales");

        var subject = await ResolveAsync(context);

        // An operator rule naming the tenant group - admitted on the tenant's own
        // trees, and evaluated with the tenant layer off - matches only a subject
        // that carries the group, so it is the stripped group that refuses here.
        var policy = CompiledPolicy.Compile(
        [
            new LatticeAuthorizationRule(
                "op-finance", LatticeSubjectSelector.Group(Finance), LatticeScope.Tree(Orders), LatticeOperation.Read, LatticeEffect.Allow),
        ]);
        var options = new LatticeAuthOptions();
        var decision = PolicyEvaluator.Evaluate(policy, options, subject, Orders, LatticeOperation.Read, "k1", null, null);
        var control = PolicyEvaluator.Evaluate(
            policy, options, subject with { GroupIds = new HashSet<string>(subject.GroupIds) { Finance } }, Orders, LatticeOperation.Read, "k1", null, null);

        Assert.Multiple(() =>
        {
            Assert.That(subject.GroupIds, Does.Not.Contain(Finance), "the asserted tenant group is stripped with the flag off");
            Assert.That(subject.GroupIds, Does.Contain("entra-sales"), "other asserted groups are kept");
            Assert.That(decision.Allowed, Is.False, "the operator rule naming the tenant group does not match");
            Assert.That(control.Allowed, Is.True, "positive control: the rule does match a subject carrying the group");
        });
    }

    [Test]
    public async Task With_the_flag_off_a_directory_recorded_tenant_group_is_kept()
    {
        using var provider = BuildTenancyProvider(enabled: false);
        var context = CreateContext(provider.GetRequiredService<ITenantGroupClaimFilter>(), directoryGroups: [Finance], Finance);

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Does.Contain(Finance), "a membership the directory records is real, not a claim");
    }

    [Test]
    public async Task With_the_flag_on_an_asserted_tenant_group_is_stripped_too()
    {
        using var provider = BuildTenancyProvider(enabled: true);
        var context = CreateContext(provider.GetRequiredService<ITenantGroupClaimFilter>(), Finance);

        var subject = await ResolveAsync(context);

        Assert.That(subject.GroupIds, Does.Not.Contain(Finance));
    }

    private static ServiceProvider BuildTenancyProvider(bool enabled)
    {
        var builder = NewBuilder();
        builder.AddLatticeTenancy(o => o.DelegatedAccessAdministrationEnabled = enabled);
        return builder.Services.BuildServiceProvider();
    }

    private static MembershipContext CreateContext(ITenantGroupClaimFilter filter, params string[] asserted) =>
        CreateContext(filter, [], asserted);

    private static MembershipContext CreateContext(ITenantGroupClaimFilter filter, string[] directoryGroups, params string[] asserted)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.GroupsOfAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyCollection<string>>(new HashSet<string>(directoryGroups, StringComparer.Ordinal)));
        directory.ExpandGroupsAsync(Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult<IReadOnlyCollection<string>>(
                new HashSet<string>(call.Arg<IReadOnlyCollection<string>>(), StringComparer.Ordinal)));
        return new MembershipContext(
            [new AssertingAuthenticator(asserted)],
            new DefaultLatticeSubjectMapper(monitor),
            directory,
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor,
            filter);
    }

    private static async Task<LatticeSubject> ResolveAsync(MembershipContext context)
    {
        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            return await context.ResolveCurrentAsync();
        }
    }

    private static ReservationSiloBuilder NewBuilder()
    {
        var builder = new ReservationSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        return builder;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class ReservationSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }

    /// <summary>Authenticates every credential of <see cref="Scheme"/> as a principal asserting fixed groups.</summary>
    private sealed class AssertingAuthenticator(IReadOnlyCollection<string> asserted) : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) => credential.Scheme == Scheme;

        public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default) =>
            new(new LatticePrincipal("mallory", Scheme, null, asserted));
    }
}
