using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression tests for tenant scoping on the live change-observation facade.
/// <see cref="LatticeStateQueryTenantScopingTests"/> pinned the same contract on the
/// read verbs; the change feed was the one per-tree verb left resolving the raw
/// caller-supplied id, so a confined tenant subscribing to <c>orders</c> tailed the
/// GLOBAL <c>orders</c> tree rather than its own <c>t/{tenant}/orders</c>.
/// </summary>
/// <remarks>
/// <para>
/// The feed matters more than the read verbs it sat beside, not less: it reads the
/// durable write-ahead log directly rather than flowing through the gated
/// <see cref="ILattice"/> surface, so there is no downstream gate to compensate for a
/// mis-resolved id. Composition therefore has to happen at the entry point, before the
/// classification check, the registry lookups, the options lookup, the visibility
/// decision, and the grain dial - all of which must see the SAME effective id.
/// </para>
/// <para>
/// The suite pins the three distinct guarantees the composition seam carries, each of
/// which was absent from this verb: namespace confinement, the fail-closed denial for
/// an unattributable caller, and the namespace-escape refusal that is the only guard
/// on the deliberately-readable <c>sys-</c> add-on trees (the tenancy access gate
/// classifies every <c>sys-</c> id as platform-owned and admits it unconditionally, so
/// nothing downstream adjudicates one).
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeStateObserverTenantScopingTests
{
    private const string Tree = "orders";
    private const string AcmeTree = "t/acme/orders";
    private const string SystemAddOnTree = "sys-tenant-registry";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static LatticeStateObserver CreateObserver(
        IGrainFactory factory, ITenantContextResolver resolver)
    {
        var services = Substitute.For<IServiceProvider>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        return new LatticeStateObserver(
            factory, options, Options.Create(new LatticeApiStateOptions()), services, resolver);
    }

    /// <summary>Wires an existing tree grain at <paramref name="treeId"/>.</summary>
    private static ILattice WireTree(IGrainFactory factory, string treeId)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        factory.GetGrain<ILattice>(treeId).Returns(lattice);
        return lattice;
    }

    /// <summary>
    /// Advances the iterator exactly one step. The verb is an
    /// <c>async IAsyncEnumerable</c>, so nothing in its body - including every check
    /// under test - runs until the first <c>MoveNextAsync</c>.
    /// </summary>
    private static async Task StartAsync(
        LatticeStateObserver observer, StateObserveRequest request, CancellationToken cancellationToken)
    {
        await using var enumerator = observer.ObserveAsync(request, cancellationToken).GetAsyncEnumerator(cancellationToken);
        await enumerator.MoveNextAsync();
    }

    [Test]
    public async Task ObserveAsync_dials_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, AcmeTree);

        // The GLOBAL tree is deliberately left unwired: had the feed resolved the raw
        // wire id it would have dialed a substitute-returning-null grain and failed
        // differently, so the assertion below distinguishes composed from uncomposed
        // rather than merely observing that something was dialed.
        var observer = CreateObserver(factory, new AmbientTenantContextResolver());

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        using (LatticeActiveTenantContext.With(Acme))
        {
            try
            {
                await StartAsync(observer, new StateObserveRequest { TreeId = Tree }, cancellation.Token);
            }
            catch (OperationCanceledException)
            {
                // The feed tails forever; the cancelled token ends the walk once the
                // resolution under test has already happened.
            }
        }

        factory.Received(1).GetGrain<ILattice>(AcmeTree);
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    [Test]
    public void ObserveAsync_denies_a_caller_the_resolver_cannot_attribute()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, AcmeTree);
        WireTree(factory, Tree);
        var observer = CreateObserver(factory, new AmbientTenantContextResolver(deny: true));

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await StartAsync(observer, new StateObserveRequest { TreeId = Tree }, CancellationToken.None),
                Throws.InstanceOf<LatticeTenantAccessDeniedException>());
        }
    }

    [Test]
    public void ObserveAsync_refuses_a_confined_tenant_naming_the_system_add_on_namespace()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, SystemAddOnTree);
        var observer = CreateObserver(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await StartAsync(
                    observer, new StateObserveRequest { TreeId = SystemAddOnTree }, CancellationToken.None),
                Throws.InstanceOf<LatticeTenantAccessDeniedException>());
        }

        // Refused before anything was dialed: the escape check lives in the composition
        // seam, which runs ahead of the existence probe.
        factory.DidNotReceive().GetGrain<ILattice>(SystemAddOnTree);
    }

    [Test]
    public async Task ObserveAsync_leaves_the_bare_name_unchanged_with_tenancy_off()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, Tree);
        var observer = CreateObserver(factory, new AmbientTenantContextResolver());

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        try
        {
            await StartAsync(observer, new StateObserveRequest { TreeId = Tree }, cancellation.Token);
        }
        catch (OperationCanceledException)
        {
            // As above - the resolution under test has already happened.
        }

        factory.Received(1).GetGrain<ILattice>(Tree);
    }
}
