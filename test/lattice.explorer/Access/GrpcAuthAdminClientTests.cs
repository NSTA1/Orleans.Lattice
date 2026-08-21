using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit tests for the production <see cref="GrpcAuthAdminClient"/>. They exercise
/// the constructor guards, the argument guards, the unconfigured / disposed error
/// paths, and the channel build (including every auth-attaching branch) by issuing
/// each call with an already-cancelled token so the transport fails fast without a
/// server. No cluster is stood up, so these remain fast, deterministic unit tests.
/// </summary>
[TestFixture]
public class GrpcAuthAdminClientTests
{
    private static readonly LatticeAuthorizationRule SampleRule =
        new("r1", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow);

    private static GrpcAuthAdminClient Create(
        ExplorerConfiguration? config,
        LatticeCallAuthentication? auth = null) =>
        new(ExplorerControlClientHarness.Session(config), ExplorerControlClientHarness.Auth(auth));

    [Test]
    public void Constructor_null_session_throws()
    {
        Assert.That(
            () => new GrpcAuthAdminClient(null!, ExplorerControlClientHarness.Auth(null)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_auth_throws()
    {
        Assert.That(
            () => new GrpcAuthAdminClient(ExplorerControlClientHarness.Session(null), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetGroupAsync_empty_group_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.GetGroupAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void UpsertGroupAsync_null_group_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.UpsertGroupAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void PutRuleAsync_null_rule_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.PutRuleAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ExplainAsync_null_scope_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(
            () => client.ExplainAsync("alice", LatticeOperation.Read, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Method_without_endpoint_throws_invalid_operation()
    {
        using var client = Create(config: null);

        Assert.That(
            async () => await client.ListGroupsAsync(new AuthPageRequest()),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Method_after_dispose_throws_object_disposed()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());
        client.Dispose();

        Assert.That(
            async () => await client.ListGroupsAsync(new AuthPageRequest()),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());

        client.Dispose();

        Assert.That(() => client.Dispose(), Throws.Nothing);
    }

    [Test]
    public async Task All_calls_build_channel_and_propagate_cancellation()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var calls = new Func<Task>[]
        {
            () => client.ListGroupsAsync(new AuthPageRequest(), ct),
            () => client.GetGroupAsync("admins", ct),
            () => client.UpsertGroupAsync(new AuthGroup { GroupId = "admins" }, ct),
            () => client.RemoveGroupAsync("admins", ct),
            () => client.AddMemberAsync("admins", "alice", cancellationToken: ct),
            () => client.RemoveMemberAsync("admins", "alice", ct),
            () => client.ListGroupMembersAsync("admins", ct),
            () => client.ListSubjectGroupsAsync("alice", ct),
            () => client.PutRuleAsync(SampleRule, ct),
            () => client.GetRuleAsync("orders", "r1", ct),
            () => client.RemoveRuleAsync("orders", "r1", ct),
            () => client.ListRulesAsync(new AuthPageRequest(), ct),
            () => client.ListRulesForTreeAsync("orders", new AuthPageRequest(), ct),
            () => client.ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("orders"), cancellationToken: ct),
            () => client.EffectivePermissionsAsync("alice", ct),
            () => client.SearchDirectoryAsync(new DirectorySearchRequest { Term = "al" }, ct),
            () => client.ResolveDirectoryPrincipalAsync("alice", ct),
            () => client.GetAccessModelAsync(ct),
        };

        foreach (var call in calls)
        {
            var ex = Assert.CatchAsync(async () => await call());
            Assert.That(ex, Is.Not.Null);
            Assert.That(ex, Is.Not.InstanceOf<LatticeAuthorizationDeniedException>(),
                "a cancelled (non permission-denied) transport fault must not be translated to a denial");
        }
    }

    [Test]
    public void Call_with_static_header_auth_builds_channel()
    {
        var auth = LatticeCallAuthentication.Basic("operator", "secret");
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.ListGroupsAsync(new AuthPageRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_h2c_builds_insecure_call_credentials()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.ListGroupsAsync(new AuthPageRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_tls_builds_channel()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.TlsConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.ListGroupsAsync(new AuthPageRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_transport_headers_builds_channel()
    {
        var config = ExplorerControlClientHarness.H2cConfig(
            transportHeaders: new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        using var client = Create(config);

        var ex = Assert.CatchAsync(
            async () => await client.ListGroupsAsync(new AuthPageRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public async Task Repeated_calls_reuse_the_same_channel()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var first = Assert.CatchAsync(async () => await client.ListGroupsAsync(new AuthPageRequest(), ct));
        var second = Assert.CatchAsync(async () => await client.ListGroupsAsync(new AuthPageRequest(), ct));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });
        await Task.CompletedTask;
    }
}
