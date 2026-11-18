using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Restoring a retained set as a whole through tree administration, and reverting
/// that restore tree by tree through the backup facade.
/// </summary>
public sealed partial class BackupSetSchedulesSectionTests
{
    private static readonly ScheduledBackupSet RetainedSet = new(
        "set-42",
        "pics-nightly-0901",
        new DateTimeOffset(2026, 9, 1, 2, 0, 0, TimeSpan.Zero),
        new DateTimeOffset(2026, 9, 1, 2, 5, 0, TimeSpan.Zero),
        true,
        [
            new ScheduledBackupSetMember(BackupScopeSelector.WholeTree("albums"), "backup-albums-1"),
            new ScheduledBackupSetMember(BackupScopeSelector.WholeTree("photos"), "backup-photos-1"),
        ]);

    private static readonly IReadOnlyList<TreeRestoreResult> Restored =
    [
        Result("albums", "backup-albums-1", 12),
        Result("photos", "backup-photos-1", 340),
    ];

    [Test]
    public void Restore_set_lists_the_trees_asks_for_the_set_name_and_restores_by_set_id()
    {
        var admin = UseTreeAdmin();
        var cut = OpenRetainedSet();

        cut.Find("button[aria-label='Restore set, set-42']").Click();
        var dialog = cut.Find("[role=alertdialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.TextContent, Does.Contain("Each live tree below is replaced by its copy in set set-42")
                .And.Contain("the restore can be reverted")
                .And.Contain("pics-nightly-0901"));
            Assert.That(
                cut.FindAll("[data-lt-set-restore-trees] li").Select(item => item.TextContent.Trim()),
                Is.EqualTo(new[] { "albums", "photos" }));
        });

        cut.Find("[role=alertdialog] input[type=text]").Input("pics-nightly-0901");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => admin.Received(1).RestoreTreeSetAsync("set-42", Arg.Any<CancellationToken>()));
        cut.WaitUntil(() =>
        {
            Assert.That(Status(cut), Is.EqualTo("Restored set pics-nightly-0901: 2 trees switched to their copies in the set."));
            var rows = cut.FindAll("[data-lt-backup-set-restore='set-42'] tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].TextContent, Does.Contain("albums").And.Contain("12").And.Contain("albums-shadow").And.Contain("albums-previous").And.Contain("Not reverted"));
            Assert.That(rows[1].TextContent, Does.Contain("photos").And.Contain("340").And.Contain("photos-shadow").And.Contain("photos-previous"));
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RevertRestoreAsync)), Is.Zero);
    }

    [Test]
    public void A_cancelled_restore_confirmation_calls_nothing()
    {
        var admin = UseTreeAdmin();
        var cut = OpenRetainedSet();

        cut.Find("button[aria-label='Restore set, set-42']").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(admin.ReceivedCalls(), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-backup-set-restore]"), Is.Empty);
        });
    }

    [Test]
    public void Revert_reverts_every_tree_through_the_backup_facade_with_its_restore_result()
    {
        UseTreeAdmin();
        var cut = RestoreTheSet();

        Button(cut, "Revert this restore").Click();

        cut.WaitUntil(() => Assert.That(Status(cut), Is.EqualTo("Reverted the restore of set pics-nightly-0901: every tree holds the copy it held before.")));
        var reverted = Backups.Calls
            .Where(call => call.Verb == nameof(ILatticeBackupControl.RevertRestoreAsync))
            .Select(call => (LatticeRestoreResult)call.Argument!)
            .ToList();
        Assert.That(reverted, Has.Count.EqualTo(2));
        for (var index = 0; index < reverted.Count; index++)
        {
            var expected = Restored[index];
            var actual = reverted[index];
            Assert.Multiple(() =>
            {
                Assert.That(actual.BackupId, Is.EqualTo(expected.BackupId));
                Assert.That(actual.TargetTreeId, Is.EqualTo(expected.TargetTreeId));
                Assert.That(actual.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
                Assert.That(actual.OperationId, Is.EqualTo(expected.OperationId));
                Assert.That(actual.ManifestChain, Is.EqualTo(expected.ManifestChain));
                Assert.That(actual.EntriesApplied, Is.EqualTo(expected.EntriesApplied));
                Assert.That(actual.ShadowPhysicalTreeId, Is.EqualTo(expected.ShadowPhysicalTreeId));
                Assert.That(actual.PreviousPhysicalTreeId, Is.EqualTo(expected.PreviousPhysicalTreeId));
            });
        }

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[data-lt-set-revert=reverted]"), Has.Count.EqualTo(2));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Revert this restore"), Is.Empty, "nothing is left to revert");
        });

        Button(cut, "Dismiss").Click();
        Assert.That(cut.FindAll("[data-lt-backup-set-restore]"), Is.Empty);
    }

    [Test]
    public void A_partial_revert_reports_the_failed_tree_keeps_the_reverted_one_and_retries_only_the_failure()
    {
        UseTreeAdmin();
        Backups.Revert = restore => restore.TargetTreeId == "photos"
            ? Task.FromException(new InvalidOperationException("previous copy purged"))
            : Task.CompletedTask;
        var cut = RestoreTheSet();

        Button(cut, "Revert this restore").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(
                cut.Find("section[aria-labelledby=lt-backups-sets-title] > p[role=alert]").TextContent,
                Is.EqualTo("Reverted 1 of 2 trees; 1 tree could not be reverted. Each tree's state is shown below."));
            Assert.That(cut.Find("[data-lt-set-revert=reverted]").ParentElement!.ParentElement!.TextContent, Does.Contain("albums"));
            Assert.That(cut.Find("[data-lt-set-revert=failed]").TextContent, Does.Contain("previous copy purged"));
        });

        Backups.Revert = _ => Task.CompletedTask;
        Button(cut, "Revert this restore").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-set-revert=reverted]"), Has.Count.EqualTo(2)));
        var trees = Backups.Calls
            .Where(call => call.Verb == nameof(ILatticeBackupControl.RevertRestoreAsync))
            .Select(call => ((LatticeRestoreResult)call.Argument!).TargetTreeId);
        Assert.That(trees, Is.EqualTo(new[] { "albums", "photos", "photos" }));
    }

    [Test]
    public void Without_tree_administration_the_restore_set_action_is_hidden()
    {
        Services.RemoveAllKeyed<ILatticeTreeAdmin>(ShellFacades.Key);

        var cut = OpenRetainedSet();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button[aria-label='Restore set, set-42']"), Is.Empty);
            Assert.That(cut.Find("[data-lt-backup-set='pics-nightly']").TextContent, Does.Contain("Each member is an ordinary backup: open it to restore that tree."));
        });
    }

    [Test]
    public void A_connection_that_does_not_serve_a_set_restore_withdraws_the_action()
    {
        var admin = UseTreeAdmin();
        admin.RestoreTreeSetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new NotSupportedException("not served"));
        var cut = OpenRetainedSet();

        ConfirmRestore(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("section[aria-labelledby=lt-backups-sets-title] > p[role=alert]").TextContent, Is.EqualTo("This connection does not serve restoring a whole set."));
            Assert.That(cut.FindAll("button[aria-label='Restore set, set-42']"), Is.Empty);
        });
    }

    [Test]
    public void A_refused_set_restore_says_why()
    {
        var admin = UseTreeAdmin();
        admin.RestoreTreeSetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException());
        var cut = OpenRetainedSet();

        ConfirmRestore(cut);

        cut.WaitUntil(() => Assert.That(
            cut.Find("section[aria-labelledby=lt-backups-sets-title] > p[role=alert]").TextContent,
            Is.EqualTo(BackupsFaults.NotPermitted)));
        Assert.That(cut.FindAll("[data-lt-backup-set-restore]"), Is.Empty);
    }

    private static TreeRestoreResult Result(string tree, string backupId, long entries) => new()
    {
        BackupId = backupId,
        TargetTreeId = tree,
        Mode = TreeRestoreMode.ShadowCutover,
        OperationId = "op-" + tree,
        ManifestChain = ["base-" + tree, backupId],
        EntriesApplied = entries,
        ShadowPhysicalTreeId = tree + "-shadow",
        PreviousPhysicalTreeId = tree + "-previous",
    };

    private static void ConfirmRestore(IRenderedComponent<BackupSchedulesPage> cut)
    {
        cut.Find("button[aria-label='Restore set, set-42']").Click();
        cut.Find("[role=alertdialog] input[type=text]").Input(RetainedSet.Name);
        cut.Find("[role=alertdialog] form").Submit();
    }

    private ILatticeTreeAdmin UseTreeAdmin()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.RestoreTreeSetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(Restored));
        Services.RemoveAllKeyed<ILatticeTreeAdmin>(ShellFacades.Key);
        Services.AddKeyedSingleton(ShellFacades.Key, admin);
        return admin;
    }

    private IRenderedComponent<BackupSchedulesPage> OpenRetainedSet()
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly, sets: [RetainedSet]);
        return OpenDetails();
    }

    private IRenderedComponent<BackupSchedulesPage> RestoreTheSet()
    {
        var cut = OpenRetainedSet();
        ConfirmRestore(cut);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-backup-set-restore='set-42'] tbody tr"), Has.Count.EqualTo(2)));
        return cut;
    }
}
