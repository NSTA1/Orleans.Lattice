using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The app-scoped backup plan computed from the manifest's tree set: rebuildable trees are
/// rederived, every other tree is restored, each named by its effective physical id.
/// </summary>
[TestFixture]
public sealed class RepoContextAppBackupPlanTests
{
    private static AppManifest Manifest() => RepoContextAppManifest.Load().Manifest!;

    [Test]
    public void Default_tenant_plan_restores_store_of_record_trees_and_rederives_derived_vector_trees()
    {
        var plan = RepoContextAppManifest.GetBackupPlan(Manifest(), TenantId.Default);

        Assert.Multiple(() =>
        {
            Assert.That(plan.TreesToRestore, Is.EqualTo(new[]
            {
                RepoContextTrees.Structural,
                RepoContextTrees.Symbol,
                RepoContextTrees.Memory,
                RepoContextTrees.Content,
                RepoContextTrees.CrossReference,
                RepoContextTrees.Session,
                RepoContextTrees.VectorPayload,
            }));
            Assert.That(plan.TreesToRederive, Is.EqualTo(new[]
            {
                RepoContextTrees.VectorMembership,
                RepoContextTrees.VectorMetadata,
                RepoContextTrees.VectorIndex,
                RepoContextTrees.VectorCoverage,
            }));
        });
    }

    [Test]
    public void Plan_partitions_exactly_the_trees_that_hold_repository_data()
    {
        var plan = RepoContextAppManifest.GetBackupPlan(Manifest(), TenantId.Default);

        Assert.Multiple(() =>
        {
            Assert.That(plan.TreesToRestore.Intersect(plan.TreesToRederive), Is.Empty);
            Assert.That(
                plan.TreesToRestore.Concat(plan.TreesToRederive).Order(StringComparer.Ordinal),
                Is.EqualTo(RepoContextTrees.AllIncludingLocalDerived.Order(StringComparer.Ordinal)));
        });
    }

    [Test]
    public void Plan_for_a_named_tenant_composes_the_tenant_namespace()
    {
        var tenant = TenantId.Parse("contoso");

        var plan = RepoContextAppManifest.GetBackupPlan(Manifest(), tenant);

        Assert.Multiple(() =>
        {
            Assert.That(plan.TreesToRestore, Does.Contain($"t/contoso/{RepoContextTrees.Memory}"));
            Assert.That(plan.TreesToRederive, Does.Contain($"t/contoso/{RepoContextTrees.VectorIndex}"));
            Assert.That(plan.TreesToRestore.Concat(plan.TreesToRederive).All(t => t.StartsWith("t/contoso/", StringComparison.Ordinal)), Is.True);
        });
    }

    [Test]
    public void Plan_names_a_structural_tree_by_its_app_namespace()
    {
        var manifest = Manifest();
        var extended = manifest with
        {
            Trees = [.. manifest.Trees, new AppTreeDeclaration { Name = "scratch", Rebuildable = true }],
        };

        var plan = RepoContextAppManifest.GetBackupPlan(extended, TenantId.Default);

        Assert.That(plan.TreesToRederive[^1], Is.EqualTo("a/repo-context/scratch"));
    }

    [Test]
    public void GetBackupPlan_rejects_a_null_manifest()
        => Assert.Throws<ArgumentNullException>(() => RepoContextAppManifest.GetBackupPlan(null!, TenantId.Default));

    [Test]
    public void GetBackupPlan_fails_closed_for_the_uninitialised_tenant()
        => Assert.Throws<LatticeTenantAccessDeniedException>(() => RepoContextAppManifest.GetBackupPlan(Manifest(), default));

    [Test]
    public void Plan_record_exposes_the_lists_it_was_built_with()
    {
        string[] restore = ["a"];
        string[] rederive = ["b"];

        var plan = new RepoContextAppBackupPlan(restore, rederive);

        Assert.Multiple(() =>
        {
            Assert.That(plan.TreesToRestore, Is.SameAs(restore));
            Assert.That(plan.TreesToRederive, Is.SameAs(rederive));
        });
    }
}
