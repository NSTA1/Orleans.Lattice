using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Regression tests for issue #1689: the external data plane must scope a
/// caller-supplied, tenant-local tree name to the caller's own tenant.
/// </summary>
/// <remarks>
/// <para>
/// The facade previously dialled <c>GetGrain&lt;ILattice&gt;(treeId)</c> with the
/// caller's name verbatim, so every tenant asking for <c>orders</c> was handed the
/// <em>same</em> physical tree. Demonstrated live: two tenants' values collided in
/// one tree and each tenant's administrator read the other's data back. A tenant
/// also could not reach its own <c>t/{tenant}/orders</c>, because the
/// reserved-namespace guard correctly refuses a caller-supplied <c>t/</c> id.
/// </para>
/// <para>
/// These tests assert on the tree id the facade actually dials, which is the
/// property that matters and the one that was wrong. The resolver is substituted so
/// each case is exact and needs no cluster.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeDataApiTenantScopingTests
{
    private const string UnqualifiedName = "orders";

    /// <summary>A resolver that composes under a fixed tenant, as the real one does.</summary>
    private sealed class FixedTenantResolver(TenantId tenant) : ITenantContextResolver
    {
        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = tenant;
            return true;
        }

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(tenant);
    }

    /// <summary>A resolver that only resolves asynchronously, exercising the cold fallback.</summary>
    private sealed class ColdTenantResolver(TenantId tenant) : ITenantContextResolver
    {
        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = default;
            return false;
        }

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(tenant);
    }

    /// <summary>A resolver that denies, as the real one does for an unauthorized assertion.</summary>
    private sealed class DenyingTenantResolver : ITenantContextResolver
    {
        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = default;
            return true;
        }

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(default(TenantId));
    }

    private static (LatticeDataApi Api, List<string> Dialled) Create(ITenantContextResolver resolver)
    {
        var dialled = new List<string>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            dialled.Add(call.ArgAt<string>(0));
            return Substitute.For<ILattice>();
        });

        var api = new LatticeDataApi(
            grainFactory,
            Options.Create(new LatticeApiDataOptions()),
            resolver);

        return (api, dialled);
    }

    [Test]
    public async Task A_write_is_scoped_into_the_callers_tenant_namespace()
    {
        var (api, dialled) = Create(new FixedTenantResolver(TenantId.Parse("acme")));

        await api.SetAsync(UnqualifiedName, "k", [1]);

        Assert.That(dialled, Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public async Task Two_tenants_using_the_same_name_reach_different_trees()
    {
        // The property the whole issue turns on: before the fix both of these
        // dialled the bare "orders" tree, so the two tenants shared one namespace.
        var (acme, acmeDialled) = Create(new FixedTenantResolver(TenantId.Parse("acme")));
        var (globex, globexDialled) = Create(new FixedTenantResolver(TenantId.Parse("globex")));

        await acme.SetAsync(UnqualifiedName, "k", [1]);
        await globex.SetAsync(UnqualifiedName, "k", [2]);

        Assert.Multiple(() =>
        {
            Assert.That(acmeDialled.Single(), Is.EqualTo("t/acme/orders"));
            Assert.That(globexDialled.Single(), Is.EqualTo("t/globex/orders"));
            Assert.That(acmeDialled.Single(), Is.Not.EqualTo(globexDialled.Single()));
        });
    }

    [Test]
    public async Task Every_write_verb_is_scoped_not_just_set()
    {
        var (api, dialled) = Create(new FixedTenantResolver(TenantId.Parse("acme")));

        await api.SetAsync(UnqualifiedName, "k", [1]);
        await api.DeleteAsync(UnqualifiedName, "k");
        await api.SetManyAsync(UnqualifiedName, [new DataEntry { Key = "k", Value = [1] }]);

        Assert.That(dialled, Is.All.EqualTo("t/acme/orders"));
    }

    [Test]
    public async Task A_crdt_verb_is_scoped_too()
    {
        var (api, dialled) = Create(new FixedTenantResolver(TenantId.Parse("acme")));

        // The substituted tree cannot satisfy the CRDT accessor chain, so the call
        // faults once it is past resolution. What matters here is which tree was
        // dialled, which is the thing that was wrong.
        try
        {
            await api.CounterIncrementAsync(UnqualifiedName, "k", "replica-1", 1);
        }
        catch (Exception ex) when (ex is not LatticeTenantAccessDeniedException)
        {
            // Expected: the double is not a real tree.
        }

        Assert.That(dialled, Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public async Task With_tenancy_off_the_bare_name_is_used_unchanged()
    {
        // The zero-cost-when-absent contract: the core no-op resolver resolves the
        // reserved default tenant and the caller's name is returned unchanged, so a
        // non-tenancy cluster behaves exactly as before.
        var (api, dialled) = Create(new FixedTenantResolver(TenantId.Default));

        await api.SetAsync(UnqualifiedName, "k", [1]);

        Assert.That(dialled, Is.EqualTo(new[] { UnqualifiedName }));
    }

    [Test]
    public async Task An_already_qualified_name_is_never_double_composed()
    {
        var (api, dialled) = Create(new FixedTenantResolver(TenantId.Parse("acme")));

        await api.SetAsync("t/acme/orders", "k", [1]);

        Assert.That(dialled, Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public void An_unresolvable_caller_fails_closed()
    {
        // A caller whose asserted tenant it may not act as resolves the "no tenant"
        // value, which must deny rather than silently fall back to a shared tree.
        var (api, dialled) = Create(new DenyingTenantResolver());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await api.SetAsync(UnqualifiedName, "k", [1]),
                Throws.TypeOf<LatticeTenantAccessDeniedException>());
            Assert.That(dialled, Is.Empty, "no tree may be dialled for a denied caller");
        });
    }

    [Test]
    public async Task The_cold_resolution_path_scopes_identically()
    {
        // Membership not warm: the facade must await the async resolution rather
        // than blocking or falling back to the bare name.
        var (api, dialled) = Create(new ColdTenantResolver(TenantId.Parse("acme")));

        await api.SetAsync(UnqualifiedName, "k", [1]);

        Assert.That(dialled, Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public void A_null_or_empty_tree_id_is_still_rejected_by_name()
    {
        var (api, _) = Create(new FixedTenantResolver(TenantId.Parse("acme")));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await api.SetAsync(null!, "k", [1]), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await api.SetAsync(string.Empty, "k", [1]), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task A_read_reports_the_caller_supplied_name_not_the_composed_one()
    {
        // The composition is internal: echoing the composed id back would leak the
        // tenant namespace into a response the caller never named that way.
        var grainFactory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(false));
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree);

        var api = new LatticeDataApi(
            grainFactory,
            Options.Create(new LatticeApiDataOptions()),
            new FixedTenantResolver(TenantId.Parse("acme")));

        var result = await api.GetAsync(UnqualifiedName, "k");

        Assert.That(result.TreeId, Is.EqualTo(UnqualifiedName));
    }
}
