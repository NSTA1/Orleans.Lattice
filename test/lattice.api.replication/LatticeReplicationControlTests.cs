using System.Collections.Generic;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationControl"/>: authorized enable /
/// disable delegate to the engine authority after authorization, an unauthorized
/// caller is denied fail-closed and the authority is never consulted, anonymous
/// callers are denied by default, the config report is permission-scoped to the
/// caller's authorized trees, and engine exceptions surface unchanged.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationControlTests
{
    private const string Tree = "orders";

    private static LatticeReplicationControl CreateControl(
        ILatticeReplicationConfigAuthority authority,
        ILatticeAccessGate gate) =>
        new(authority, new ReplicationAccessAuthorizer(gate, membership: null));

    [Test]
    public async Task EnableReplicationAsync_authorized_delegates_to_authority_and_maps_result()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .EnableReplicationAsync(Tree, LatticeMergeMode.OrSet, "west", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new LatticeReplicationEnableResult(Tree, LatticeMergeMode.OrSet, false, true)));
        var control = CreateControl(authority, new AllowingAccessGate());

        var result = await control.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet, "west");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(result.BootstrapRequested, Is.True);
        });
        await authority.Received(1)
            .EnableReplicationAsync(Tree, LatticeMergeMode.OrSet, "west", Arg.Any<CancellationToken>());
    }

    [Test]
    public void EnableReplicationAsync_unauthorized_denies_and_never_calls_authority()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        var control = CreateControl(authority, new DenyingAccessGate("no grant"));

        Assert.That(
            async () => await control.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(
            authority.ReceivedCalls().Any(),
            Is.False,
            "the authority must not be consulted when authorization fails");
    }

    [Test]
    public void EnableReplicationAsync_anonymous_denied_by_default()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        var control = CreateControl(authority, new AnonymousDenyingAccessGate());

        Assert.That(
            async () => await control.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(authority.ReceivedCalls().Any(), Is.False);
    }

    [Test]
    public async Task DisableReplicationAsync_authorized_delegates_to_authority_and_maps_result()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .DisableReplicationAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new LatticeReplicationDisableResult(Tree, true)));
        var control = CreateControl(authority, new AllowingAccessGate());

        var result = await control.DisableReplicationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.AlreadyDisabled, Is.True);
        });
        await authority.Received(1).DisableReplicationAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public void DisableReplicationAsync_unauthorized_denies_and_never_calls_authority()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        var control = CreateControl(authority, new DenyingAccessGate("no grant"));

        Assert.That(
            async () => await control.DisableReplicationAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(authority.ReceivedCalls().Any(), Is.False);
    }

    [Test]
    public async Task GetReplicationConfigAsync_returns_only_authorized_trees()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .GetAllTreeStatusesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(
                new Dictionary<string, LatticeReplicationTreeStatus>
                {
                    ["orders"] = new("orders", true, LatticeMergeMode.OrSet, false),
                    ["secret"] = new("secret", true, LatticeMergeMode.LwwRegister, false),
                    ["inventory"] = new("inventory", false, null, true),
                }));
        // Caller may manage only "orders" and "inventory".
        var control = CreateControl(authority, new TreeScopedAccessGate("orders", "inventory"));

        var report = await control.GetReplicationConfigAsync();

        var treeIds = report.Trees.Select(t => t.TreeId).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Is.EquivalentTo(new[] { "orders", "inventory" }));
            Assert.That(treeIds, Does.Not.Contain("secret"));
            var inventory = report.Trees.Single(t => t.TreeId == "inventory");
            Assert.That(inventory.Enabled, Is.False);
            Assert.That(inventory.Mode, Is.Null);
            Assert.That(inventory.Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task GetReplicationConfigAsync_empty_when_no_trees_configured()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .GetAllTreeStatusesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(
                new Dictionary<string, LatticeReplicationTreeStatus>()));
        var control = CreateControl(authority, new AllowingAccessGate());

        var report = await control.GetReplicationConfigAsync();

        Assert.That(report.Trees, Is.Empty);
    }

    [Test]
    public async Task GetReplicationConfigAsync_hides_all_trees_when_caller_unauthorized()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .GetAllTreeStatusesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(
                new Dictionary<string, LatticeReplicationTreeStatus>
                {
                    ["orders"] = new("orders", true, LatticeMergeMode.OrSet, false),
                }));
        var control = CreateControl(authority, new DenyingAccessGate("no grant"));

        var report = await control.GetReplicationConfigAsync();

        Assert.That(report.Trees, Is.Empty);
    }

    [Test]
    public void EnableReplicationAsync_surfaces_precondition_exception()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .EnableReplicationAsync(Tree, LatticeMergeMode.RwFlag, null, Arg.Any<CancellationToken>())
            .Returns<Task<LatticeReplicationEnableResult>>(_ => throw new LatticeReplicationPreconditionFailedException(
                "no local replica id", Tree, LatticeMergeMode.RwFlag));
        var control = CreateControl(authority, new AllowingAccessGate());

        Assert.That(
            async () => await control.EnableReplicationAsync(Tree, LatticeMergeMode.RwFlag),
            Throws.TypeOf<LatticeReplicationPreconditionFailedException>());
    }

    [Test]
    public void EnableReplicationAsync_surfaces_mode_change_exception()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .EnableReplicationAsync(Tree, LatticeMergeMode.OrSet, null, Arg.Any<CancellationToken>())
            .Returns<Task<LatticeReplicationEnableResult>>(_ => throw new LatticeReplicationModeChangeRejectedException(
                "disable then re-enable", Tree, LatticeMergeMode.OrSet, LatticeMergeMode.LwwRegister, false));
        var control = CreateControl(authority, new AllowingAccessGate());

        Assert.That(
            async () => await control.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet),
            Throws.TypeOf<LatticeReplicationModeChangeRejectedException>());
    }

    [Test]
    public void EnableReplicationAsync_null_tree_throws()
    {
        var control = CreateControl(Substitute.For<ILatticeReplicationConfigAuthority>(), new AllowingAccessGate());

        Assert.That(
            async () => await control.EnableReplicationAsync(null!, LatticeMergeMode.OrSet),
            Throws.ArgumentNullException);
    }

    [Test]
    public void DisableReplicationAsync_empty_tree_throws()
    {
        var control = CreateControl(Substitute.For<ILatticeReplicationConfigAuthority>(), new AllowingAccessGate());

        Assert.That(
            async () => await control.DisableReplicationAsync(string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_null_authority_throws()
    {
        Assert.That(
            () => new LatticeReplicationControl(null!, new ReplicationAccessAuthorizer(new AllowingAccessGate())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_authorizer_throws()
    {
        Assert.That(
            () => new LatticeReplicationControl(Substitute.For<ILatticeReplicationConfigAuthority>(), null!),
            Throws.ArgumentNullException);
    }
}
