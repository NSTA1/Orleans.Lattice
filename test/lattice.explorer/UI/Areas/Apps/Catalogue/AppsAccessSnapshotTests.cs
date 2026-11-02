using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

[TestFixture]
public sealed class AppsAccessSnapshotTests
{
    [TestCaseSource(nameof(CapabilityCombinations))]
    public void Capabilities_every_combination_requires_all_prerequisites(int flags)
    {
        var sources = (flags & 1) != 0;
        var available = (flags & 2) != 0;
        var describe = (flags & 4) != 0;
        var install = (flags & 8) != 0;
        var list = (flags & 16) != 0;
        var workspace = (flags & 32) != 0;
        var snapshot = new AppsAccessSnapshot
        {
            Catalog = new LatticeAppCatalogCapabilities
            {
                CanListSources = sources,
                CanListAvailable = available,
                CanDescribeFromSource = describe,
            },
            Control = new LatticeAppsCapabilities { CanInstall = install, CanList = list },
            WorkspaceServed = workspace,
        };

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.CanBrowseCatalogue, Is.EqualTo(sources && available));
            Assert.That(snapshot.CanReview, Is.EqualTo(sources && available && describe));
            Assert.That(snapshot.CanInstall, Is.EqualTo(sources && available && describe && install));
            Assert.That(snapshot.IsVisible, Is.EqualTo(workspace || (sources && available) || list));
        });
    }

    [Test]
    public void Denied_defaults_grant_nothing_and_contain_no_apps()
    {
        var snapshot = AppsAccessSnapshot.Denied;

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.CanBrowseCatalogue, Is.False);
            Assert.That(snapshot.CanReview, Is.False);
            Assert.That(snapshot.CanInstall, Is.False);
            Assert.That(snapshot.IsVisible, Is.False);
            Assert.That(snapshot.WorkspaceServed, Is.False);
            Assert.That(snapshot.MyApps, Is.Empty);
            Assert.That(snapshot.Installed, Is.Empty);
            Assert.That(snapshot.InTenant, Is.Empty);
            Assert.That(snapshot.Updates, Is.Empty);
            Assert.That(snapshot.FailedActivations, Is.Empty);
        });
    }

    [Test]
    public void Installed_every_lifecycle_is_filtered_without_mutating_the_snapshot()
    {
        var installed = Enum.GetValues<AppLifecycleState>()
            .Select(state => new AppSummary
            {
                Slug = state.ToString(),
                Version = "1.0.0",
                State = state,
                Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "tests" },
            })
            .Reverse()
            .ToImmutableArray();
        var snapshot = new AppsAccessSnapshot { Installed = installed };

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.InTenant.Select(app => app.Slug),
                Is.EqualTo(installed.Where(app => app.State != AppLifecycleState.Uninstalled)
                    .Select(app => app.Slug).Order(StringComparer.Ordinal)));
            Assert.That(snapshot.InTenant, Has.Length.EqualTo(installed.Length - 1));
            Assert.That(snapshot.FailedActivations.Select(app => app.State),
                Is.EqualTo(new[] { AppLifecycleState.Failed }));
            Assert.That(snapshot.Installed, Is.EqualTo(installed));
        });
    }

    [Test]
    public void ShowsSameAs_is_true_for_equal_content_in_different_instances()
    {
        Assert.That(Sample().ShowsSameAs(Sample()), Is.True);
        Assert.That(AppsAccessSnapshot.Denied.ShowsSameAs(new AppsAccessSnapshot { MyApps = default }), Is.True, "a default list reads as empty");
    }

    [Test]
    public void ShowsSameAs_is_false_for_any_difference_the_caller_would_see()
    {
        var sample = Sample();
        var different = new[]
        {
            sample with { MyApps = [] },
            sample with { MyApps = [sample.MyApps[0] with { Roles = ["viewer", "editor"] }] },
            sample with { MyApps = [sample.MyApps[0] with { Version = "1.1.0" }] },
            sample with { MyApps = [sample.MyApps[0] with { InstallRevision = 2 }] },
            sample with { MyApps = [sample.MyApps[0] with { HasUi = false }] },
            sample with { Installed = [sample.Installed[0] with { State = AppLifecycleState.Disabled }] },
            sample with { Installed = [sample.Installed[0] with { Version = "1.1.0" }] },
            sample with { Updates = [] },
            sample with { Updates = [sample.Updates[0] with { NewestVersion = "1.2.0" }] },
            sample with { Control = new LatticeAppsCapabilities { CanList = true } },
            sample with { Catalog = new LatticeAppCatalogCapabilities { CanListSources = true } },
            sample with { WorkspaceServed = false },
            sample with { ProbeUnanswered = true },
        };

        Assert.That(different.Select(snapshot => sample.ShowsSameAs(snapshot)), Is.All.False);
        Assert.That(() => sample.ShowsSameAs(null!), Throws.ArgumentNullException);
    }

    private static AppsAccessSnapshot Sample() => new()
    {
        Control = new LatticeAppsCapabilities { CanList = true, CanInstall = true },
        WorkspaceServed = true,
        MyApps = [new WorkspaceAppSummary { Slug = "task-board", Version = "1.0.0", InstallRevision = 1, HasUi = true, Roles = ["viewer"] }],
        Installed =
        [
            new AppSummary
            {
                Slug = "task-board",
                Version = "1.0.0",
                State = AppLifecycleState.Enabled,
                Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "tests" },
            },
        ],
        Updates = [new AvailableAppSummary { SourceKey = "in-image", Slug = "task-board", NewestVersion = "1.1.0", InstalledVersion = "1.0.0" }],
    };

    private static IEnumerable<int> CapabilityCombinations() => Enumerable.Range(0, 64);
}
