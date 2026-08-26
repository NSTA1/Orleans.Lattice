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
/// <b>source</b> of <see cref="ILattice.MergeAsync"/>. A merge drains every entry
/// of a caller-supplied source tree into the calling tree, where it is then
/// readable under the calling tree's own read policy - so a merge whose source
/// names a reserved tree would launder control-plane or cross-tenant state past
/// the protection of the namespace it lives in.
/// <para>
/// The guard rejects a user-origin source in the internal <c>_lattice_</c>
/// namespace, the dogfooded <c>sys-</c> system-data namespace (authorization
/// policy, membership graph, backup catalogs), and the structural <c>t/</c>
/// tenant namespace, and is suppressed under a system-origin scope so first-party
/// machinery is unaffected. It is the cheap namespace half of the fix; the
/// per-caller authorization half (the access gate consulted for the source tree)
/// is covered by <c>AccessGateKeyFilterIntegrationTests</c>.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainMergeSourceGuardTests
{
    private const string OrdinaryTreeId = "app-attacker";

    /// <summary>
    /// Reserved source ids a user-origin merge must never be able to drain. The
    /// membership and policy trees are the highest-value targets: they hold the
    /// inputs to every authorization decision.
    /// </summary>
    private static readonly string[] ReservedSourceIds =
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

    [TestCaseSource(nameof(ReservedSourceIds))]
    public void MergeAsync_rejects_a_reserved_source_tree(string sourceTreeId)
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrain(OrdinaryTreeId).MergeAsync(sourceTreeId));

        Assert.That(ex!.Message, Does.Contain(sourceTreeId),
            "the rejection names the refused source so an operator can see what was blocked");
    }

    [Test]
    public void MergeAsync_rejects_a_null_source_tree()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => CreateGrain(OrdinaryTreeId).MergeAsync(null!));

    [Test]
    public void MergeAsync_reserved_source_guard_is_suppressed_under_system_origin()
    {
        // First-party machinery that legitimately composes a reserved id runs
        // system-origin; the guard must not fence it out. The call still fails
        // (the substituted factory has no real merge coordinator), but it must
        // not fail with the reserved-source rejection.
        using var scope = LatticeAccessGateContext.EnterSystemOrigin();

        InvalidOperationException? reserved = null;
        try
        {
            CreateGrain(OrdinaryTreeId).MergeAsync("sys-auth-policy").GetAwaiter().GetResult();
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("is reserved", StringComparison.Ordinal))
        {
            reserved = ex;
        }
        catch
        {
            // Any other failure is the substituted infrastructure, not the guard.
        }

        Assert.That(reserved, Is.Null, "a system-origin merge is not fenced by the reserved-source guard");
    }
}
