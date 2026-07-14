using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="AuthAdminMcpPermissionResolver"/>, the default
/// resolver that maps a caller's effective authorization rules onto the four MCP
/// facade groups. Proves the allow-grant projection, the deny non-subtraction at
/// discovery time, and the fail-closed behaviour when the auth facade is missing,
/// the subject is unresolved, or the introspection throws.
/// </summary>
[TestFixture]
public sealed class AuthAdminMcpPermissionResolverTests
{
    private static AuthAdminMcpPermissionResolver CreateResolver(ILatticeAuthAdmin? admin)
    {
        var services = new ServiceCollection();
        if (admin is not null)
        {
            services.AddSingleton(admin);
        }

        return new AuthAdminMcpPermissionResolver(
            services.BuildServiceProvider(),
            NullLogger<AuthAdminMcpPermissionResolver>.Instance);
    }

    private static LatticeAuthorizationRule Rule(LatticeOperation operations, LatticeEffect effect)
        => new(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Tree("orders"),
            operations: operations,
            effect: effect);

    private static ILatticeAuthAdmin AdminReturning(params LatticeAuthorizationRule[] rules)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new AuthEffectivePermissions { SubjectId = "alice", Rules = rules });
        return admin;
    }

    [Test]
    public async Task Read_grant_makes_state_and_data_usable_only()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public async Task Write_grant_makes_data_usable_but_not_state()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Write, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.False,
                "Write does not intersect the read-only state mask.");
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public async Task Admin_grant_makes_auth_usable_only()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Admin, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.False);
        });
    }

    [Test]
    public async Task Backup_grant_makes_backup_usable_only()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Restore, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public async Task Multiple_grants_union_their_groups()
    {
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Read, LatticeEffect.Allow),
            Rule(LatticeOperation.Admin, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.False);
        });
    }

    [Test]
    public async Task Deny_rule_does_not_grant_a_group()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Admin, LatticeEffect.Deny)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True,
            "Discovery advertises on Allow-grant presence; a lone Deny grants nothing.");
    }

    [Test]
    public async Task No_rules_grants_nothing()
    {
        var resolver = CreateResolver(AdminReturning());

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True);
    }

    [Test]
    public async Task Missing_auth_facade_fails_closed()
    {
        var resolver = CreateResolver(admin: null);

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True,
            "With no ILatticeAuthAdmin registered the resolver must grant no group.");
    }

    [Test]
    public async Task Empty_subject_id_fails_closed_without_calling_the_facade()
    {
        var admin = AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow));
        var resolver = CreateResolver(admin);

        var access = await resolver.ResolveAsync(new LatticeCredential(string.Empty), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True);
        await admin.DidNotReceive().EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Introspection_failure_fails_closed()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(_ => throw new InvalidOperationException("boom"));
        var resolver = CreateResolver(admin);

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True,
            "A resolution failure must fail closed rather than grant an unscoped set.");
    }

    [Test]
    public async Task Prefers_principal_id_over_token_as_subject()
    {
        var admin = AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow));
        var resolver = CreateResolver(admin);

        await resolver.ResolveAsync(
            new LatticeCredential("the-token", principalId: "the-principal"),
            CancellationToken.None);

        await admin.Received(1).EffectivePermissionsAsync("the-principal", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Falls_back_to_token_when_no_principal_id()
    {
        var admin = AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow));
        var resolver = CreateResolver(admin);

        await resolver.ResolveAsync(new LatticeCredential("the-token"), CancellationToken.None);

        await admin.Received(1).EffectivePermissionsAsync("the-token", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new AuthAdminMcpPermissionResolver(
                null!, NullLogger<AuthAdminMcpPermissionResolver>.Instance));
            Assert.Throws<ArgumentNullException>(() => new AuthAdminMcpPermissionResolver(
                new ServiceCollection().BuildServiceProvider(), null!));
        });
    }
}
