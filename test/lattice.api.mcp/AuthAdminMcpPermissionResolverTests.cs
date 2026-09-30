using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        => CreateResolver(admin, NullLogger<AuthAdminMcpPermissionResolver>.Instance);

    private static AuthAdminMcpPermissionResolver CreateResolver(
        ILatticeAuthAdmin? admin,
        ILogger<AuthAdminMcpPermissionResolver> logger)
    {
        var services = new ServiceCollection();
        if (admin is not null)
        {
            services.AddSingleton(admin);
        }

        return new AuthAdminMcpPermissionResolver(services.BuildServiceProvider(), logger);
    }

    private static LatticeAuthorizationRule Rule(LatticeOperation operations, LatticeEffect effect)
        => new(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Tree("orders"),
            operations: operations,
            effect: effect);

    private static LatticeAuthorizationRule ClusterWideRule(LatticeOperation operations, LatticeEffect effect)
        => new(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.ClusterWide(),
            operations: operations,
            effect: effect);

    private static ILatticeAuthAdmin AdminReturning(params LatticeAuthorizationRule[] rules)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
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
        await admin.DidNotReceive().EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Introspection_failure_fails_closed()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(_ => throw new InvalidOperationException("boom"));
        var resolver = CreateResolver(admin);

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True,
            "A resolution failure must fail closed rather than grant an unscoped set.");
    }

    [TestCase(StatusCode.Cancelled)]
    [TestCase(StatusCode.DeadlineExceeded)]
    [TestCase(StatusCode.Unavailable)]
    [TestCase(StatusCode.Internal)]
    public void A_transient_backend_fault_surfaces_a_retryable_error_not_an_empty_grant_set(StatusCode status)
    {
        // The answer never arrived, so there is no permission set to report.
        // Returning an empty one would answer tools/list SUCCESSFULLY with a single
        // meta-tool, which a client cannot tell apart from a genuine revocation -
        // the observed failure had a platform administrator advertised one tool
        // instead of 147 with no error anywhere on the wire.
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(
                _ => throw new RpcException(new Status(status, "backend stalled")));
        var resolver = CreateResolver(admin);

        Assert.That(
            async () => await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None),
            Throws.TypeOf<LatticeApiMcpDiscoveryUnavailableException>());
    }

    [Test]
    public void An_orleans_response_timeout_surfaces_a_retryable_error()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(
                _ => throw new TimeoutException("Response did not arrive on response id 42."));
        var resolver = CreateResolver(admin);

        Assert.That(
            async () => await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None),
            Throws.TypeOf<LatticeApiMcpDiscoveryUnavailableException>());
    }

    [TestCase(StatusCode.PermissionDenied)]
    [TestCase(StatusCode.Unauthenticated)]
    [TestCase(StatusCode.NotFound)]
    [TestCase(StatusCode.InvalidArgument)]
    public async Task An_authoritative_denial_still_fails_closed(StatusCode status)
    {
        // The backend replied, and the reply denies. Fail-closed is correct here and
        // must not be traded for a retryable error.
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(
                _ => throw new RpcException(new Status(status, "denied")));
        var resolver = CreateResolver(admin);

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.IsEmpty, Is.True);
    }

    [Test]
    public async Task Prefers_principal_id_over_token_as_subject()
    {
        var admin = AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow));
        var resolver = CreateResolver(admin);

        await resolver.ResolveAsync(
            new LatticeCredential("the-token", principalId: "the-principal"),
            CancellationToken.None);

        await admin.Received(1).EffectivePermissionsAsync("the-principal", Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Falls_back_to_token_when_no_principal_id()
    {
        var admin = AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow));
        var resolver = CreateResolver(admin);

        await resolver.ResolveAsync(new LatticeCredential("the-token"), CancellationToken.None);

        await admin.Received(1).EffectivePermissionsAsync("the-token", Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Security regression. The lookup key is deliberately unchanged - a host may
    /// have provisioned rules against whatever its bridge puts in the credential,
    /// so narrowing it would silently revoke access - but the value that gets
    /// <b>written out</b> must not be the caller's bearer secret. Both failure
    /// arms log the subject, and when no principal id was resolved that subject
    /// was the token itself, so an ordinary backend blip rested a live credential
    /// in the server's logs for as long as they are retained.
    /// </summary>
    [Test]
    public async Task A_failure_log_never_carries_the_raw_token_as_the_subject()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(_ => throw new InvalidOperationException("boom"));
        var logger = new CapturingLogger();
        var resolver = CreateResolver(admin, logger);

        await resolver.ResolveAsync(new LatticeCredential("super-secret-token"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(logger.Messages, Is.Not.Empty, "the fail-closed arm must still say why it failed");
            Assert.That(
                logger.Messages.Any(m => m.Contains("super-secret-token", StringComparison.Ordinal)),
                Is.False,
                "a bearer token must never be written to the log");
            Assert.That(
                logger.Messages.Any(m => m.Contains("token:", StringComparison.Ordinal)),
                Is.True,
                "the fingerprint still identifies the caller in the log");
        });
    }

    [Test]
    public async Task A_transient_failure_log_never_carries_the_raw_token_as_the_subject()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.EffectivePermissionsAsync(Arg.Any<string>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthEffectivePermissions>>(
                _ => throw new RpcException(new Status(StatusCode.Unavailable, "backend stalled")));
        var logger = new CapturingLogger();
        var resolver = CreateResolver(admin, logger);

        try
        {
            await resolver.ResolveAsync(new LatticeCredential("super-secret-token"), CancellationToken.None);
        }
        catch (LatticeApiMcpDiscoveryUnavailableException)
        {
            // Expected: the retryable arm is asserted elsewhere; what matters here
            // is what it wrote on the way out.
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.Messages, Is.Not.Empty);
            Assert.That(
                logger.Messages.Any(m => m.Contains("super-secret-token", StringComparison.Ordinal)),
                Is.False,
                "a bearer token must never be written to the log");
        });
    }

    /// <summary>Captures formatted log messages so a test can assert on what was written.</summary>
    private sealed class CapturingLogger : ILogger<AuthAdminMcpPermissionResolver>
    {
        private readonly List<string> _messages = [];

        public IReadOnlyList<string> Messages => _messages;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => _messages.Add(formatter(state, exception));
    }

    [Test]
    public async Task Telemetry_grant_over_cluster_wide_scope_makes_telemetry_usable_only()
    {
        var resolver = CreateResolver(AdminReturning(
            ClusterWideRule(LatticeOperation.Telemetry, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.State), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Backup), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public async Task Telemetry_grant_over_a_tree_scope_does_not_make_telemetry_usable()
    {
        // Telemetry is a cluster-wide capability with no tree to scope it to, so a
        // tree-scoped rule that happens to carry the bit is a data-plane grant on
        // that one tree - not a grant of the scopeless capability. LatticeScope's
        // own contract says a data-plane rule on a tree can never confer it.
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Telemetry, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False,
            "A rule scoped to a single tree must not grant the cluster-wide telemetry group.");
    }

    [Test]
    public async Task Telemetry_over_a_tree_scope_is_not_carried_into_the_granted_operations()
    {
        // Group membership is only half the gate: per-tool filtering consults
        // GrantedOperations, so leaking the bit there would re-open every telemetry
        // tool on a set whose group membership was correctly withheld.
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Read | LatticeOperation.Telemetry, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.Telemetry), Is.False,
                "The cluster-wide bit must be masked off a tree-scoped rule before it is carried.");
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.Read), Is.True,
                "Masking must remove only the cluster-wide-only bits, never the rule's data-plane grant.");
        });
    }

    [Test]
    public async Task A_tree_scoped_telemetry_grant_does_not_suppress_a_cluster_wide_one()
    {
        // The mask must not turn into a denial: a caller holding both rules is
        // entitled to telemetry by the cluster-wide one.
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Telemetry, LatticeEffect.Allow),
            ClusterWideRule(LatticeOperation.Telemetry, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.True);
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.Telemetry), Is.True);
        });
    }

    /// <summary>
    /// Security regression. <see cref="LatticeOperation.AppInstall"/> is the second
    /// scopeless cluster-wide capability (the authorizer resolves it over the
    /// cluster-wide tree id), so it must be masked off a tree-scoped rule exactly
    /// as telemetry is. Omitting it from the mask let a rule on any single tree
    /// carry the installation capability into the granted operation set.
    /// </summary>
    [Test]
    public async Task App_install_over_a_tree_scope_is_not_carried_into_the_granted_operations()
    {
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Read | LatticeOperation.AppInstall, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.AppInstall), Is.False,
                "AppInstall names no tree, so a tree-scoped rule must not carry it.");
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.Read), Is.True,
                "Masking must remove only the cluster-wide-only bits, never the rule's data-plane grant.");
        });
    }

    [Test]
    public async Task App_install_over_a_cluster_wide_scope_is_carried_into_the_granted_operations()
    {
        // The mask must not turn into a denial: a cluster-wide rule is the scope
        // the capability is actually authorized over, so it has to survive.
        var resolver = CreateResolver(AdminReturning(
            ClusterWideRule(LatticeOperation.AppInstall, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.AppInstall), Is.True);
    }

    /// <summary>
    /// Security regression. The cluster-wide predicate must test the scope's
    /// <see cref="LatticeScopeKind"/> as well as its tree id. Only
    /// <see cref="LatticeScope.ClusterWide"/> - a Tree-kind scope over the
    /// sentinel - is authorizable cluster-wide, but the scope constructor also
    /// admits a key- or prefix-kind scope on the same sentinel and nothing
    /// rejects one at authoring time. Such a rule grants nothing at the gate,
    /// because a scopeless capability is requested with no key and the evaluator
    /// consults the tree tier only, so honouring it here advertised a facade the
    /// caller could never invoke.
    /// </summary>
    [TestCase(LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.AppInstall)]
    public async Task A_key_scoped_wildcard_rule_does_not_confer_a_scopeless_capability(
        LatticeOperation scopeless)
    {
        var resolver = CreateResolver(AdminReturning(new LatticeAuthorizationRule(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Key(LatticeScope.ClusterWideTreeId, "k"),
            operations: scopeless,
            effect: LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.GrantedOperations.HasFlag(scopeless), Is.False,
            "A key-kind scope on the wildcard tree id is not a cluster-wide grant.");
    }

    [Test]
    public async Task A_key_scoped_wildcard_telemetry_rule_does_not_advertise_the_telemetry_group()
    {
        // The group half of the same break: advertising it would put the telemetry
        // tools in the session collection - and so within reach of tools/call -
        // while the facade's own gate denies every invocation.
        var resolver = CreateResolver(AdminReturning(new LatticeAuthorizationRule(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Key(LatticeScope.ClusterWideTreeId, "k"),
            operations: LatticeOperation.Telemetry,
            effect: LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False,
            "Advertising the group would break lock-step with a gate that denies it.");
    }

    [Test]
    public async Task A_prefix_scoped_wildcard_rule_does_not_confer_a_scopeless_capability()
    {
        var resolver = CreateResolver(AdminReturning(new LatticeAuthorizationRule(
            ruleId: "r-" + Guid.NewGuid().ToString("N"),
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Prefix(LatticeScope.ClusterWideTreeId, "p"),
            operations: LatticeOperation.Telemetry,
            effect: LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.GrantedOperations.HasFlag(LatticeOperation.Telemetry), Is.False);
            Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False);
        });
    }

    [Test]
    public async Task A_tree_scoped_rule_still_grants_its_data_plane_groups()
    {
        // The scope predicate is narrow by design: it withholds only the
        // capabilities that have no tree to scope to, and leaves ordinary
        // tree-scoped authorization exactly as it was.
        var resolver = CreateResolver(AdminReturning(
            Rule(LatticeOperation.Read | LatticeOperation.Write, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.Contains(LatticeApiMcpGroup.Data), Is.True,
                "A tree-scoped read/write grant must still open the data group.");
            Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False);
        });
    }

    [Test]
    public async Task Read_grant_does_not_make_telemetry_usable()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Read, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False,
            "A read grant must not expose the scopeless telemetry group (fail-closed).");
    }

    [Test]
    public async Task Admin_grant_does_not_make_telemetry_usable()
    {
        var resolver = CreateResolver(AdminReturning(Rule(LatticeOperation.Admin, LatticeEffect.Allow)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False,
            "Telemetry must be granted explicitly; not even an administrator grant confers it.");
    }

    [Test]
    public async Task Telemetry_deny_grant_does_not_make_telemetry_usable()
    {
        var resolver = CreateResolver(AdminReturning(
            ClusterWideRule(LatticeOperation.Telemetry, LatticeEffect.Deny)));

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False);
    }

    [Test]
    public async Task Anonymous_caller_sees_no_group_including_telemetry()
    {
        var resolver = CreateResolver(AdminReturning());

        var access = await resolver.ResolveAsync(new LatticeCredential("alice"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(access.IsEmpty, Is.True);
            Assert.That(access.Contains(LatticeApiMcpGroup.Telemetry), Is.False);
        });
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
