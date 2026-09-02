using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Security regression coverage for the reserved-namespace guard on the
/// <b>destination</b> of <see cref="ILattice.SnapshotAsync"/>. A snapshot does not
/// merely read: it creates and registers its caller-supplied destination tree and
/// seeds it with the calling tree's content. An unguarded destination therefore
/// lets a caller holding nothing more than administration of one ordinary tree
/// plant a fully populated, registered tree inside a namespace the public surface
/// otherwise refuses to create in - squatting a <c>sys-</c> control-plane id
/// before the owning add-on lazily creates it, or materialising a tree under
/// another tenant's <c>t/</c> prefix.
/// <para>
/// The guard mirrors the one already enforced on the source of
/// <see cref="ILattice.MergeAsync"/> (see
/// <see cref="LatticeGrainMergeSourceGuardTests"/>): it rejects a user-origin
/// destination in the internal <c>_lattice_</c> namespace, the dogfooded
/// <c>sys-</c> system-data namespace, and a foreign tenant's <c>t/</c> namespace,
/// while admitting the active tenant's own prefix and staying suppressed under a
/// system-origin scope so first-party machinery is unaffected.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainSnapshotDestinationGuardTests
{
    private const string OrdinaryTreeId = "app-attacker";

    /// <summary>
    /// Reserved destination ids a user-origin snapshot must never be able to
    /// create. The policy and membership ids are the highest-value targets: they
    /// hold the inputs to every authorization decision, and are created lazily on
    /// first write, so squatting one wins the race against its owning add-on.
    /// </summary>
    private static readonly string[] ReservedDestinationIds =
    [
        "sys-auth-policy",
        "sys-membership-groups",
        "sys-membership-edges",
        "sys-backup-catalog",
        "t/victim/orders",
        "_lattice_registry",
    ];

    private static ILattice CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    [TestCaseSource(nameof(ReservedDestinationIds))]
    public void SnapshotAsync_rejects_a_reserved_destination_tree(string destinationTreeId)
    {
        var ex = Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrain(OrdinaryTreeId).SnapshotAsync(destinationTreeId, SnapshotMode.Offline));

        Assert.That(ex!.Message, Does.Contain(destinationTreeId),
            "the rejection names the refused destination so an operator can see what was blocked");
    }

    [Test]
    public void SnapshotAsync_rejects_a_null_destination_tree()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => CreateGrain(OrdinaryTreeId).SnapshotAsync(null!, SnapshotMode.Offline));

    [Test]
    public void SnapshotAsync_rejects_a_reserved_destination_before_registering_anything()
    {
        // The damage a snapshot does is registration plus seeding, so the guard has to
        // fire ahead of both. Proving no registry call was made is what distinguishes
        // a real guard from an error raised after the tree already exists.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", OrdinaryTreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));

        var grain = new LatticeGrain(
            context,
            grainFactory,
            optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory),
            Substitute.For<IServiceProvider>(),
            NullLogger<LatticeGrain>.Instance);

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SnapshotAsync("sys-auth-policy", SnapshotMode.Offline));

        registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public void SnapshotAsync_admits_a_destination_owned_by_the_active_tenant()
    {
        // The guard fences a foreign tenant's namespace, not the caller's own: a
        // tenant-scoped caller must still be able to snapshot within its own prefix.
        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        InvalidOperationException? reserved = null;
        try
        {
            CreateGrain(OrdinaryTreeId).SnapshotAsync("t/acme/orders", SnapshotMode.Offline).GetAwaiter().GetResult();
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("is reserved", StringComparison.Ordinal))
        {
            reserved = ex;
        }
        catch
        {
            // Any other failure is the substituted infrastructure, not the guard.
        }

        Assert.That(reserved, Is.Null, "the active tenant's own namespace is not fenced");
    }

    [Test]
    public void SnapshotAsync_reserved_destination_guard_is_suppressed_under_system_origin()
    {
        // First-party machinery that legitimately composes a reserved id runs
        // system-origin; the guard must not fence it out. The call still fails
        // (the substituted factory has no real snapshot coordinator), but it must
        // not fail with the reserved-destination rejection.
        using var scope = LatticeAccessGateContext.EnterSystemOrigin();

        InvalidOperationException? reserved = null;
        try
        {
            CreateGrain(OrdinaryTreeId).SnapshotAsync("sys-auth-policy", SnapshotMode.Offline).GetAwaiter().GetResult();
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("is reserved", StringComparison.Ordinal))
        {
            reserved = ex;
        }
        catch
        {
            // Any other failure is the substituted infrastructure, not the guard.
        }

        Assert.That(reserved, Is.Null, "a system-origin snapshot is not fenced by the reserved-destination guard");
    }
}
