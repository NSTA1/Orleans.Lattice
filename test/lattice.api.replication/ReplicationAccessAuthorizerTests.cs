using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationAccessAuthorizer"/>: it authorizes the
/// whole tree for the dedicated <see cref="LatticeOperation.Replication"/>
/// capability, throws fail-closed on a denial, exposes a non-throwing probe for
/// permission-scoped discovery, denies anonymous callers by default, and guards
/// its arguments.
/// </summary>
[TestFixture]
public sealed class ReplicationAccessAuthorizerTests
{
    private const string Tree = "orders";

    [Test]
    public void Constructor_null_gate_throws()
    {
        Assert.That(() => new ReplicationAccessAuthorizer(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task AuthorizeAsync_allows_when_gate_grants()
    {
        var authorizer = new ReplicationAccessAuthorizer(new AllowingAccessGate());

        await authorizer.AuthorizeAsync(Tree);

        Assert.Pass();
    }

    [Test]
    public void AuthorizeAsync_throws_when_gate_denies()
    {
        var authorizer = new ReplicationAccessAuthorizer(new DenyingAccessGate("no grant"));

        Assert.That(
            async () => await authorizer.AuthorizeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeAsync_denies_anonymous_by_default()
    {
        var authorizer = new ReplicationAccessAuthorizer(new AnonymousDenyingAccessGate(), membership: null);

        Assert.That(
            async () => await authorizer.AuthorizeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task IsAuthorizedAsync_true_when_granted_false_when_denied()
    {
        var granted = new ReplicationAccessAuthorizer(new TreeScopedAccessGate(Tree));

        Assert.Multiple(async () =>
        {
            Assert.That(await granted.IsAuthorizedAsync(Tree), Is.True);
            Assert.That(await granted.IsAuthorizedAsync("other"), Is.False);
        });
    }

    [Test]
    public void AuthorizeAsync_null_tree_throws()
    {
        var authorizer = new ReplicationAccessAuthorizer(new AllowingAccessGate());

        Assert.That(async () => await authorizer.AuthorizeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void IsAuthorizedAsync_empty_tree_throws()
    {
        var authorizer = new ReplicationAccessAuthorizer(new AllowingAccessGate());

        Assert.That(async () => await authorizer.IsAuthorizedAsync(string.Empty), Throws.ArgumentException);
    }
}
