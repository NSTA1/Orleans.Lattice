using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

[TestFixture]
public sealed class AppUpgradeDiffTests
{
    [TestCase("tree-added")]
    [TestCase("tree-removed")]
    [TestCase("role-added")]
    [TestCase("role-removed")]
    [TestCase("role-changed")]
    [TestCase("role-scope-changed")]
    [TestCase("bridge-removed")]
    public void Diff_structural_change_is_not_empty_and_does_not_require_new_consent(string change)
    {
        var installed = AppsTestData.TaskBoard();
        var next = installed with { Version = "2.0.0" };
        next = change switch
        {
            "tree-added" => next with { Trees = next.Trees.Add(new AppTreeDescriptor { Name = "archive" }) },
            "tree-removed" => next with { Trees = [] },
            "role-added" => next with { Roles = next.Roles.Add(next.Roles[0] with { Name = "reader" }) },
            "role-removed" => next with { Roles = next.Roles.RemoveAt(0) },
            "role-changed" => next with
            {
                Roles = next.Roles.SetItem(0, next.Roles[0] with { Operations = LatticeOperation.Read }),
            },
            "role-scope-changed" => next with
            {
                Roles = next.Roles.SetItem(0, next.Roles[0] with
                {
                    Scopes = [new AppRoleScope
                    {
                        Tree = "tasks",
                        Kind = LatticeScopeKind.Prefix,
                        KeyOrPrefix = "open/",
                    }],
                }),
            },
            "bridge-removed" => next with { Ui = next.Ui! with { Bridge = [] } },
            _ => throw new ArgumentOutOfRangeException(nameof(change)),
        };

        var diff = AppConsentAnalysis.Diff(installed, next, consent: null);

        Assert.Multiple(() =>
        {
            Assert.That(diff.IsEmpty, Is.False, change);
            Assert.That(diff.RequiresReconsent, Is.False, change);
            Assert.That(diff.CeilingAdded, Is.EqualTo(LatticeOperation.None));
            Assert.That(diff.ScopesAdded, Is.Empty);
            Assert.That(diff.BridgeAdded, Is.Empty);
            Assert.That(diff.TreesAdded, Is.EqualTo(change == "tree-added" ? new[] { "archive" } : []));
            Assert.That(diff.TreesRemoved, Is.EqualTo(change == "tree-removed" ? new[] { "tasks" } : []));
            Assert.That(diff.RolesAdded, Is.EqualTo(change == "role-added" ? new[] { "reader" } : []));
            Assert.That(diff.RolesRemoved, Is.EqualTo(change == "role-removed" ? new[] { "viewer" } : []));
            Assert.That(diff.RolesChanged, Is.EqualTo(change is "role-changed" or "role-scope-changed" ? new[] { "viewer" } : []));
            Assert.That(diff.BridgeRemoved.Length, Is.EqualTo(change == "bridge-removed" ? 3 : 0));
        });
    }

    [TestCase("operations")]
    [TestCase("scope")]
    [TestCase("bridge")]
    public void Diff_each_consent_gap_independently_requires_reconsent(string gap)
    {
        var installed = AppsTestData.TaskBoard();
        var consent = new AppConsentReport
        {
            Slug = installed.Slug,
            Version = installed.Version,
            Ceiling = new AppCapabilityCeilingDescriptor
            {
                AllowedOperations = gap == "operations"
                    ? AppConsentAnalysis.RequiredOperations(installed) & ~LatticeOperation.Write
                    : AppConsentAnalysis.RequiredOperations(installed),
            },
            BridgeGrants = gap == "bridge" ? [] : installed.Ui!.Bridge,
        };
        var next = AppsTestData.TaskBoard("2.0.0", crossApp: gap == "scope");

        var diff = AppConsentAnalysis.Diff(installed, next, consent);

        Assert.Multiple(() =>
        {
            Assert.That(diff.RequiresReconsent, Is.True, gap);
            Assert.That(diff.IsEmpty, Is.False, gap);
            Assert.That(diff.CeilingAdded, Is.EqualTo(gap == "operations" ? LatticeOperation.Write : LatticeOperation.None));
            Assert.That(diff.ScopesAdded.Length, Is.EqualTo(gap == "scope" ? 1 : 0));
            Assert.That(diff.BridgeAdded.Length, Is.EqualTo(gap == "bridge" ? 3 : 0));
        });
    }
}
