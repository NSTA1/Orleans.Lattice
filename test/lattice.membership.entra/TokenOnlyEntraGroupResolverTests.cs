namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="TokenOnlyEntraGroupResolver"/>, the dependency-free
/// default overage resolver that echoes back the token-asserted groups without
/// any out-of-band lookup.
/// </summary>
public class TokenOnlyEntraGroupResolverTests
{
    [Test]
    public async Task ResolveGroupsAsync_echoes_token_asserted_groups()
    {
        var resolver = new TokenOnlyEntraGroupResolver();
        var context = new EntraGroupResolutionContext(
            "alice-oid",
            EntraTestAuthority.TenantId,
            new[] { "group-a", "group-b" });

        var resolved = await resolver.ResolveGroupsAsync(context);

        Assert.That(resolved, Is.EquivalentTo(new[] { "group-a", "group-b" }));
    }

    [Test]
    public async Task ResolveGroupsAsync_returns_empty_when_token_carried_no_groups()
    {
        var resolver = new TokenOnlyEntraGroupResolver();
        var context = new EntraGroupResolutionContext("alice-oid");

        var resolved = await resolver.ResolveGroupsAsync(context);

        Assert.That(resolved, Is.Empty);
    }

    [Test]
    public void ResolveGroupsAsync_null_context_throws()
    {
        var resolver = new TokenOnlyEntraGroupResolver();

        Assert.That(
            async () => await resolver.ResolveGroupsAsync(null!),
            Throws.ArgumentNullException);
    }
}
