using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextGrant"/> and the seed step of
/// <see cref="RepoContextStartupService"/>: the granted operation mask matches the
/// repository-context tool surface plus both capabilities the orphaned-leaf repair
/// is gated on, and warmup seeds exactly one Allow rule per tree scoped to that
/// tree for the local agent.
/// </summary>
[TestFixture]
public sealed class RepoContextStartupServiceTests
{
    [Test]
    public void Grant_covers_the_full_repository_context_data_plane_mask_plus_the_repair_capabilities()
    {
        const LatticeOperation expected =
            LatticeOperation.Read
            | LatticeOperation.Write
            | LatticeOperation.Delete
            | LatticeOperation.RangeRead
            | LatticeOperation.RangeDelete
            | LatticeOperation.CrdtApply
            | LatticeOperation.AtomicWrite
            | LatticeOperation.BulkLoad
            | LatticeOperation.TreeLifecycle
            | LatticeOperation.Admin;

        Assert.That(RepoContextGrant.Operations, Is.EqualTo(expected));
    }

    /// <summary>
    /// Pins <b>both</b> capabilities the orphaned-leaf repair is gated on, separately
    /// from the exact-equality assertion above, so a regression reports the cause
    /// rather than only a changed mask.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The repair crosses two independent gates, and each one alone is insufficient:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <c>LatticeTreeAdmin.RepairOrphanedLeavesAsync</c> enforces whole-tree
    ///     <see cref="LatticeOperation.TreeLifecycle"/> through
    ///     <c>TreeAdminAccessAuthorizer.AuthorizeTreeLifecycleAsync</c>.
    ///   </description></item>
    ///   <item><description>
    ///     <c>LatticeGrain.DriveOrphanedLeafPassAsync</c> then enforces whole-tree
    ///     <see cref="LatticeOperation.Admin"/> for the non-dry-run pass, because it
    ///     removes leaves. The sibling inspection verb takes the identical path under
    ///     <see cref="LatticeOperation.Read"/>, which is why the audit was reachable
    ///     while the repair was not.
    ///   </description></item>
    /// </list>
    /// <para>
    /// Missing either bit leaves the tool advertised and then refused, which is
    /// precisely the state #3289 chose to avoid by leaving it unregistered.
    /// </para>
    /// </remarks>
    [Test]
    public void Grant_carries_both_capabilities_the_orphaned_leaf_repair_is_gated_on()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextGrant.Operations.HasFlag(LatticeOperation.TreeLifecycle),
                Is.True,
                "The facade enforces whole-tree TreeLifecycle before the call reaches the grain.");
            Assert.That(
                RepoContextGrant.Operations.HasFlag(LatticeOperation.Admin),
                Is.True,
                "The grain enforces a second whole-tree gate on Admin for the non-dry-run pass.");
        });

    /// <summary>
    /// Records that <see cref="LatticeOperation.Admin"/> is <b>not</b> what makes the
    /// tree-administration tool group discoverable, so a future narrowing cannot cite
    /// discovery as the reason the bit has to stay.
    /// </summary>
    /// <remarks>
    /// The group is discovered through
    /// <c>LatticeApiMcpGroupCapabilityMap.RequiredOperations</c>, whose TreeAdmin mask
    /// is <c>Admin | TreeLifecycle | BulkLoad | Restore</c> and is matched
    /// <b>disjunctively</b> by <c>AuthAdminMcpPermissionResolver.GroupIsGranted</c>
    /// (<c>(rule.Operations &amp; mask) != None</c>). The grant intersected that mask
    /// through <see cref="LatticeOperation.BulkLoad"/> long before it carried
    /// <c>Admin</c>, which is why the group was discoverable - and the orphaned-leaf
    /// audit advertised - with no <c>Admin</c> anywhere in it. <c>Admin</c> is carried
    /// for the grain's <b>invocation</b> gate alone, and the XML docs on
    /// <c>TreeAdminToolGroup</c> claiming the group is discovered only by an
    /// <c>Admin</c>-granted caller are simply wrong.
    /// </remarks>
    [Test]
    public void Administrator_capability_is_not_what_makes_the_tree_admin_group_discoverable()
    {
        const LatticeOperation withoutAdmin = RepoContextGrant.Operations & ~LatticeOperation.Admin;

        Assert.That(
            withoutAdmin & LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.TreeAdmin),
            Is.Not.EqualTo(LatticeOperation.None),
            "The group mask is matched disjunctively, so the grant reaches treeadmin through BulkLoad alone.");
    }

    [Test]
    public async Task SeedAccessAsync_puts_one_allow_rule_per_tree_for_the_local_agent()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var captured = new List<LatticeAuthorizationRule>();
        await store.PutRuleAsync(
            Arg.Do<LatticeAuthorizationRule>(captured.Add),
            Arg.Any<CancellationToken>());

        var service = CreateService(store);

        await service.SeedAccessAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(captured, Has.Count.EqualTo(RepoContextHostTrees.All.Count));
            Assert.That(
                captured.Select(r => r.Scope.TreeId),
                Is.EquivalentTo(RepoContextHostTrees.All));
            Assert.That(captured, Has.All.Property(nameof(LatticeAuthorizationRule.Effect)).EqualTo(LatticeEffect.Allow));
            Assert.That(
                captured,
                Has.All.Property(nameof(LatticeAuthorizationRule.Operations)).EqualTo(RepoContextGrant.Operations));
            Assert.That(
                captured,
                Has.All.Property(nameof(LatticeAuthorizationRule.Subject))
                    .Property(nameof(LatticeSubjectSelector.Id)).EqualTo(LocalTrustedAgent.SubjectId));
        });
    }

    [Test]
    public async Task WarmupAsync_marks_ready_after_a_successful_seed()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);

        await service.WarmupAsync(CancellationToken.None);

        Assert.That(readiness.IsReady, Is.True);
    }

    [Test]
    public async Task SeedAccessAsync_opts_the_symbol_tree_in_to_schema_versioning_when_unversioned()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.GetVersionConfigAsync(RepoContextHostTrees.Symbol, Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);

        var service = CreateService(store, admin: admin);
        await service.SeedAccessAsync(CancellationToken.None);

        await admin.Received(1).SetVersionConfigAsync(
            RepoContextHostTrees.Symbol,
            Arg.Is<LatticeSchemaVersionConfig>(c =>
                c.SchemaId == RepoContextHostTrees.SymbolSchemaId
                && c.TargetVersion == RepoContextHostTrees.SymbolSchemaVersion),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SeedAccessAsync_leaves_an_already_versioned_symbol_tree_untouched()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.GetVersionConfigAsync(RepoContextHostTrees.Symbol, Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(
                RepoContextHostTrees.SymbolSchemaId, RepoContextHostTrees.SymbolSchemaVersion));

        var service = CreateService(store, admin: admin);
        await service.SeedAccessAsync(CancellationToken.None);

        await admin.DidNotReceive().SetVersionConfigAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var logger = Substitute.For<ILogger<RepoContextStartupService>>();
        var readiness = new RepoContextReadinessState();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextStartupService(null!, admin, readiness, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, null!, readiness, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, null!, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, readiness, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, readiness, lifetime, null!),
                Throws.ArgumentNullException);
        });
    }

    private static RepoContextStartupService CreateService(
        ILatticeAuthorizationPolicyStore store,
        RepoContextReadinessState? readiness = null,
        ILatticeSchemaVersionAdmin? admin = null)
        => new(
            store,
            admin ?? Substitute.For<ILatticeSchemaVersionAdmin>(),
            readiness ?? new RepoContextReadinessState(),
            Substitute.For<IHostApplicationLifetime>(),
            Substitute.For<ILogger<RepoContextStartupService>>());
}
