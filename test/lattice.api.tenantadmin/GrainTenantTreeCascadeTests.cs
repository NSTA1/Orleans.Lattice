using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="GrainTenantTreeCascade"/>, the production
/// <see cref="ITenantTreeCascade"/> that the tenant-delete path uses to
/// soft-delete a tenant's trees. Every other tenant-admin fixture substitutes
/// the cascade seam, so these are the only tests that exercise the real
/// registry enumeration, the per-id ownership re-check, and the ambient scopes
/// the enumeration and the deletes run under.
/// </summary>
[TestFixture]
public sealed class GrainTenantTreeCascadeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    /// <summary>
    /// Wires a substituted grain factory so the registry resolves to
    /// <paramref name="registry"/> and every <see cref="ILattice"/> id resolves to
    /// a recording tree double, and returns the tree doubles keyed by tree id.
    /// </summary>
    private static (IGrainFactory Factory, Dictionary<string, RecordingTree> Trees) Wire(ILatticeRegistry registry)
    {
        var factory = Substitute.For<IGrainFactory>();
        var trees = new Dictionary<string, RecordingTree>(StringComparer.Ordinal);

        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var id = call.ArgAt<string>(0);
            if (!trees.TryGetValue(id, out var tree))
            {
                tree = new RecordingTree();
                trees[id] = tree;
            }

            return tree.Grain;
        });

        return (factory, trees);
    }

    /// <summary>Builds a registry double returning exactly <paramref name="ids"/> for any prefix.</summary>
    private static ILatticeRegistry RegistryReturning(params string[] ids)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(Task.FromResult<IReadOnlyList<string>>(ids));
        return registry;
    }

    [Test]
    public void Constructor_rejects_a_null_grain_factory()
        => Assert.Throws<ArgumentNullException>(() => _ = new GrainTenantTreeCascade(null!));

    [Test]
    public async Task DeleteTenantTreesAsync_scopes_the_registry_scan_to_the_tenant_prefix()
    {
        var registry = RegistryReturning();
        var (factory, _) = Wire(registry);

        await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        // The prefix is what turns a full catalog walk into a bounded range scan.
        await registry.Received(1).GetAllTreeIdsAsync("t/acme/");
    }

    [Test]
    public async Task DeleteTenantTreesAsync_returns_zero_and_touches_no_tree_when_the_tenant_owns_none()
    {
        var (factory, trees) = Wire(RegistryReturning());

        var deleted = await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(deleted, Is.Zero);
        Assert.That(trees, Is.Empty, "no ILattice grain may be resolved when the tenant owns no trees");
    }

    [Test]
    public async Task DeleteTenantTreesAsync_soft_deletes_every_owned_tree_and_returns_the_count()
    {
        var (factory, trees) = Wire(RegistryReturning("t/acme/orders", "t/acme/users"));

        var deleted = await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(deleted, Is.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(trees["t/acme/orders"].Deletes, Is.EqualTo(1));
            Assert.That(trees["t/acme/users"].Deletes, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DeleteTenantTreesAsync_skips_an_id_that_is_not_tenant_scoped()
    {
        // A legacy cluster-global id has no t/ prefix, so TryGetTenant fails and
        // the id must be skipped rather than deleted under the tenant's cascade.
        var (factory, trees) = Wire(RegistryReturning("legacy-global", "t/acme/orders"));

        var deleted = await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(deleted, Is.EqualTo(1));
        Assert.That(trees.ContainsKey("legacy-global"), Is.False,
            "a non-tenant-scoped id must never be resolved or deleted by the cascade");
    }

    [Test]
    public async Task DeleteTenantTreesAsync_skips_a_tree_owned_by_a_different_tenant()
    {
        // The prefix scan is only a performance hint: ownership is confirmed per
        // id, so an id belonging to another tenant is never cascaded even if the
        // registry hands it back.
        var (factory, trees) = Wire(RegistryReturning("t/beta/orders", "t/acme/orders"));

        var deleted = await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(deleted, Is.EqualTo(1));
        Assert.That(trees.ContainsKey("t/beta/orders"), Is.False,
            "another tenant's tree must never be deleted by this tenant's cascade");
    }

    [Test]
    public async Task DeleteTenantTreesAsync_skips_a_malformed_tenant_scoped_id()
    {
        // "t/acme" has no tenant-local name segment, so it is not a well-formed
        // tenant tree id and must be skipped by the ownership re-check.
        var (factory, trees) = Wire(RegistryReturning("t/acme", "t/acme/orders"));

        var deleted = await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(deleted, Is.EqualTo(1));
        Assert.That(trees.ContainsKey("t/acme"), Is.False);
    }

    [Test]
    public async Task DeleteTenantTreesAsync_enumerates_under_system_origin_with_no_ambient_tenant()
    {
        // The registry read must be admitted as trusted infrastructure and must
        // not be re-pruned to whichever tenant happens to be ambient, which need
        // not be the delete target.
        var registry = Substitute.For<ILatticeRegistry>();
        bool? systemOriginDuringScan = null;
        var ambientDuringScan = (TenantId?)TenantId.Parse("someone-else");
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(_ =>
        {
            systemOriginDuringScan = LatticeSystemOrigin.IsActive;
            ambientDuringScan = LatticeActiveTenantContext.Current;
            return Task.FromResult<IReadOnlyList<string>>([]);
        });
        var (factory, _) = Wire(registry);

        using (LatticeActiveTenantContext.With(TenantId.Parse("someone-else")))
        {
            await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);
        }

        Assert.Multiple(() =>
        {
            Assert.That(systemOriginDuringScan, Is.True);
            Assert.That(ambientDuringScan, Is.Null);
        });
    }

    [Test]
    public async Task DeleteTenantTreesAsync_restores_the_ambient_scopes_it_entered()
    {
        var (factory, _) = Wire(RegistryReturning("t/acme/orders"));

        await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeSystemOrigin.IsActive, Is.False);
            Assert.That(LatticeActiveTenantContext.Current, Is.Null);
        });
    }

    [Test]
    public async Task DeleteTenantTreesAsync_deletes_each_tree_under_system_origin()
    {
        // The delete must be admitted as trusted infrastructure past the
        // tenant-namespace user-write guard.
        var (factory, trees) = Wire(RegistryReturning("t/acme/orders"));

        await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme);

        Assert.That(trees["t/acme/orders"].SystemOriginOnDelete, Is.True);
    }

    [Test]
    public async Task DeleteTenantTreesAsync_forwards_the_cancellation_token_to_each_delete()
    {
        var (factory, trees) = Wire(RegistryReturning("t/acme/orders"));
        using var cts = new CancellationTokenSource();

        await new GrainTenantTreeCascade(factory).DeleteTenantTreesAsync(Acme, cts.Token);

        Assert.That(trees["t/acme/orders"].LastToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public void DeleteTenantTreesAsync_rejects_the_uninitialised_no_tenant_value()
        => Assert.ThrowsAsync<ArgumentException>(async () =>
            await new GrainTenantTreeCascade(Wire(RegistryReturning()).Factory)
                .DeleteTenantTreesAsync(default));

    /// <summary>
    /// A substituted <see cref="ILattice"/> paired with the observations the
    /// cascade's only call - <c>DeleteTreeAsync</c> - makes visible: the number of
    /// deletes, the cancellation token forwarded, and whether the ambient
    /// system-origin scope was active at the moment of the delete.
    /// </summary>
    private sealed class RecordingTree
    {
        public RecordingTree()
        {
            Grain = Substitute.For<ILattice>();
            Grain.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(call =>
            {
                Deletes++;
                SystemOriginOnDelete = LatticeSystemOrigin.IsActive;
                LastToken = call.ArgAt<CancellationToken>(0);
                return Task.CompletedTask;
            });
        }

        public ILattice Grain { get; }

        public int Deletes { get; private set; }

        public bool SystemOriginOnDelete { get; private set; }

        public CancellationToken LastToken { get; private set; }
    }
}
