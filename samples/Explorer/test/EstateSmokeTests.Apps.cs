using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Samples.Explorer.TaskBoard;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// The operator's Apps area in tenant acme, where the seeder installed and enabled
/// the task board: the catalogue describes the installed version as one its own
/// install owns, and Apps lists it as installed in acme.
/// </summary>
public sealed partial class EstateSmokeTests
{
    [Test]
    public async Task The_catalogue_reports_no_ownership_conflict_for_the_task_board_installed_in_acme()
    {
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.Administrator, SampleIdentities.AcmeTenant);
        var board = (await circuit.Apps.ListAsync()).Apps.Single(app => app.Slug == TaskBoardApp.Slug);

        var described = await circuit.Catalog.DescribeFromSourceAsync(board.Provenance.Source, board.Slug, board.Version);

        Assert.That(described, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(described!.State, Is.EqualTo(AppLifecycleState.Enabled), "the catalogue sees the install the seeder enabled");
            Assert.That(
                described.Trees.Select(tree => tree.OwnershipConflict),
                Is.All.Null,
                "a tree the install already owns is not a conflict");
        });
    }

    [Test]
    public async Task The_seeded_task_board_is_recorded_under_the_provenance_the_in_image_source_vouches_for()
    {
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.Administrator, SampleIdentities.AcmeTenant);
        var recorded = (await circuit.Apps.ListAsync()).Apps.Single(app => app.Slug == TaskBoardApp.Slug).Provenance;
        var vouched = (await circuit.Catalog.DescribeFromSourceAsync(InImageAppSource.SourceKey, TaskBoardApp.Slug))!.Provenance;

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Source, Is.EqualTo(InImageAppSource.SourceKey));
            Assert.That(recorded.Publisher, Is.EqualTo(vouched.Publisher), "the seeder installs as the Apps area does, under the source's publisher");
            Assert.That(recorded.Reference, Is.EqualTo(vouched.Reference));
        });
    }

    [Test]
    public async Task An_install_from_the_in_image_source_is_the_owner_of_the_seeded_task_boards_trees()
    {
        var slug = AppSlug.Parse(TaskBoardApp.Slug);
        var resolved = await _sample.East.Services.GetRequiredService<AppSourceSet>().ResolveAsync(slug, sourceKey: InImageAppSource.SourceKey);

        IReadOnlyList<AppTreeOwnershipConflict> conflicts;
        using (LatticeSystemOrigin.Enter())
        {
            // The probe as an install or upgrade from the source would claim: the source's publisher, no override.
            conflicts = await _sample.East.Services.GetRequiredService<IAppRegistry>()
                .GetTreeOwnershipConflictsAsync(TenantId.Parse(SampleIdentities.AcmeTenant), resolved.Manifest!, resolved.Provenance!);
        }

        Assert.That(conflicts, Is.Empty, "the seeded install owns its trees as the in-image source's publisher, so an upgrade from it is not refused");
    }

    [Test]
    public async Task The_operator_sees_the_task_board_installed_in_acme()
    {
        var (address, apps) = await SampleTestHost.GetPageAsync(_sample, "t/acme/apps");

        Assert.Multiple(() =>
        {
            Assert.That(address.AbsolutePath, Is.EqualTo("/t/acme/apps"));
            Assert.That(apps, Does.Contain("Installed in tenant acme"), "the operator's Apps lists the tenant's installed apps");
            Assert.That(apps, Does.Contain($"data-lt-installed-app=\"{TaskBoardApp.Slug}\""), "and the task board is one of them");
        });
    }
}
