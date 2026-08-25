namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="NullTreePlacementResolver"/>, the core no-op seam
/// that keeps registration byte-for-byte identical to pre-placement behaviour
/// when the tenancy add-on is not registered.
/// </summary>
public sealed class NullTreePlacementResolverTests
{
    private static ITreePlacementResolver CreateResolver() => new NullTreePlacementResolver();

    [Test]
    public void TryResolveForRegistration_resolves_synchronously_to_default()
    {
        var resolver = CreateResolver();

        var resolved = resolver.TryResolveForRegistration("any-tree", out var placement);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
        });
    }

    [Test]
    public void TryResolveForRegistration_resolves_a_tenant_scoped_id_to_default_too()
    {
        // The null seam is tenancy-agnostic: even a t/ id resolves to the baseline
        // because core never reads a tenant registry.
        var resolver = CreateResolver();

        var resolved = resolver.TryResolveForRegistration("t/acme/orders", out var placement);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(placement.WalProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        });
    }

    [Test]
    public async Task ResolveForRegistrationAsync_returns_default()
    {
        var resolver = CreateResolver();

        var placement = await resolver.ResolveForRegistrationAsync("any-tree");

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_completes_synchronously()
    {
        // The cached ValueTask must complete synchronously so the null-seam fast
        // path allocates no async state machine.
        var resolver = CreateResolver();

        var pending = resolver.ResolveForRegistrationAsync("any-tree");

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        await pending;
    }

    [Test]
    public void TryResolveForRegistration_null_tree_id_throws()
    {
        var resolver = CreateResolver();

        Assert.That(
            () => resolver.TryResolveForRegistration(null!, out _),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveForRegistrationAsync_null_tree_id_throws()
    {
        var resolver = CreateResolver();

        Assert.That(
            async () => await resolver.ResolveForRegistrationAsync(null!),
            Throws.ArgumentNullException);
    }
}
