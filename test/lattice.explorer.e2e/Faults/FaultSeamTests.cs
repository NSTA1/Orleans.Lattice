using System.Text;
using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Faults;

/// <summary>
/// Each fault and diagnostic seam of the estate harness produces the state it claims
/// (#5359, acceptance): the identity seam refuses a wrong password, a dropped gateway call
/// fails once, an activation is located on a running silo, an injected orphan is found by
/// the audit and repaired, a backup's manifest can be deleted behind the catalogue, and a
/// restarted silo and Explorer head rejoin the region.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class FaultSeamTests
{
    private static JourneyEstate Estate => FaultEstate.Current;

    private static LatticeDataApiGrpcClient Data(string user = JourneyEstateOptions.Administrator, string? password = null) =>
        LatticeDataApiGrpcClient.Create(Estate.Invoker(user, password), JourneyEstate.Serialization);

    /// <summary>
    /// The silo checks the password against the directory: the right one is the administrator,
    /// a wrong one is nobody, and nobody may write.
    /// </summary>
    [Test]
    [Order(1)]
    public async Task A_wrong_password_is_refused_and_the_right_one_admitted()
    {
        var request = new DataSetRequest { TreeId = "seam-identity", Key = "k", Value = [1] };

        var refused = Assert.ThrowsAsync<RpcException>(() => Data(password: "not-the-password").SetAsync(request));
        Assert.That(refused!.StatusCode, Is.AnyOf(StatusCode.PermissionDenied, StatusCode.Unauthenticated));

        await Data().SetAsync(request);
        var read = await Data().GetAsync(new DataGetRequest { TreeId = "seam-identity", Key = "k" });
        Assert.That(read.Found, Is.True);
    }

    /// <summary>The next matching call through the gateway is torn down; the one after it succeeds.</summary>
    [Test]
    [Order(2)]
    public async Task A_dropped_gateway_call_fails_once()
    {
        await Data().SetAsync(new DataSetRequest { TreeId = "seam-drop", Key = "k", Value = [1] });
        await Estate.FailNextGatewayCallAsync("/orleans.lattice.api.data/Get");

        var get = new DataGetRequest { TreeId = "seam-drop", Key = "k" };
        Assert.ThrowsAsync<RpcException>(() => Data().GetAsync(get), "the armed call is dropped");
        Assert.That((await Data().GetAsync(get)).Found, Is.True, "the next call goes through");
    }

    /// <summary>A tree's grain activation is reported on one of the region's running silos.</summary>
    [Test]
    [Order(3)]
    public async Task An_activation_is_located_on_a_running_silo()
    {
        await Data().SetAsync(new DataSetRequest { TreeId = "seam-locate", Key = "k", Value = [1] });

        var silo = await Estate.LocateActivationAsync("lattice", "seam-locate");

        Assert.That(silo, Is.Not.Null, "the tree's grain is active somewhere");
        Assert.That(Estate.WestRegion.Silos[silo!.Value].IsRunning, Is.True);
        Assert.That(await Estate.LocateActivationAsync("lattice", "seam-never-touched"), Is.Null);
    }

    /// <summary>An injected orphan is what the audit finds, the repair removes, and a re-audit no longer sees.</summary>
    [Test]
    [Order(4)]
    public async Task An_injected_orphan_is_found_by_the_audit_and_repaired()
    {
        const string tree = "seam-orphans";
        for (var i = 0; i < 20; i++)
        {
            await Data().SetAsync(new DataSetRequest { TreeId = tree, Key = $"key-{i:D3}", Value = Encoding.UTF8.GetBytes($"value-{i:D3}") });
        }

        var admin = LatticeTreeAdminApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);
        Assert.That((await AuditAsync(admin, tree)).OrphanedLeafCount, Is.Zero, "a fresh tree has no orphan");

        var orphan = await Estate.InjectOrphanedLeafAsync(tree);
        var audit = await AuditAsync(admin, tree);
        Assert.Multiple(() =>
        {
            Assert.That(orphan, Is.Not.Empty);
            Assert.That(audit.OrphanedLeafCount, Is.EqualTo(1), "the audit finds the injected leaf");
            Assert.That(audit.RepairableCount, Is.EqualTo(1), "every row it holds is readable by descent");
        });

        string? resume = null;
        do
        {
            var repaired = await admin.RepairOrphanedLeavesAsync(tree, resume);
            resume = repaired.ResumeFrom;
        }
        while (resume is not null);

        Assert.That((await AuditAsync(admin, tree)).OrphanedLeafCount, Is.Zero, "the repair removed it");
        var read = await Data().GetAsync(new DataGetRequest { TreeId = tree, Key = "key-007" });
        Assert.That(Encoding.UTF8.GetString(read.Value), Is.EqualTo("value-007"), "and no row was lost");
    }

    /// <summary>
    /// A backup's manifest is deleted from the sink behind the catalogue's back: the blob is
    /// gone and the catalogue still lists the backup - a genuine orphan row.
    /// </summary>
    [Test]
    [Order(5)]
    public async Task A_deleted_backup_manifest_leaves_an_orphan_catalogue_row()
    {
        const string tree = "seam-backup";
        await Data().SetAsync(new DataSetRequest { TreeId = tree, Key = "k", Value = [1] });
        var backups = LatticeBackupApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);
        await backups.StartBackupAsync(new LatticeBackupCaptureRequest("seam-backup-1", BackupScopeSelector.WholeTree(tree)));

        var backup = await WaitForAsync(async () =>
            (await backups.ListBackupsAsync(new BackupCatalogRequest { PageSize = 50, NamePrefix = "seam-backup-1" })).Entries.FirstOrDefault());
        var manifest = $"manifests/{backup.Id}";
        Assert.That(await Estate.BackupBlobsAsync(backup.Id), Does.Contain(manifest), "the capture wrote its manifest");

        Assert.That(await Estate.DeleteBackupArtifactAsync(backup.Id), Is.EqualTo(manifest));

        Assert.That(await Estate.BackupBlobsAsync(backup.Id), Does.Not.Contain(manifest), "the blob is gone");
        var listed = await backups.ListBackupsAsync(new BackupCatalogRequest { PageSize = 50, NamePrefix = "seam-backup-1" });
        Assert.That(listed.Entries.Select(entry => entry.Id), Does.Contain(backup.Id), "the catalogue still lists it");
    }

    /// <summary>A silo and the Explorer head are restarted, and the region serves again through both silos.</summary>
    [Test]
    [Order(6)]
    public async Task A_restarted_silo_and_explorer_rejoin_the_region()
    {
        await Estate.RestartSiloAsync(JourneyEstate.West, 1);
        await Estate.WaitForGatewayToMatchSilosAsync(timeout: TimeSpan.FromMinutes(1));
        Assert.That((await Estate.GatewayDestinationsAsync()).All(destination => destination.Healthy), Is.True);

        await Estate.RestartExplorerAsync(JourneyEstate.West);
        Assert.That(Estate.WestRegion.ExplorerProcesses[0].IsRunning, Is.True);

        await Data().SetAsync(new DataSetRequest { TreeId = "seam-restart", Key = "k", Value = [1] });
        Assert.That((await Data().GetAsync(new DataGetRequest { TreeId = "seam-restart", Key = "k" })).Found, Is.True);
    }

    private static async Task<(int OrphanedLeafCount, int RepairableCount)> AuditAsync(LatticeTreeAdminApiGrpcClient admin, string tree)
    {
        var (orphans, repairable) = (0, 0);
        string? resume = null;
        do
        {
            var batch = await admin.AuditOrphanedLeavesAsync(tree, resume);
            Assert.That(batch.VerdictComplete, Is.True, "the audit judged every region it reached");
            orphans += batch.OrphanedLeafCount;
            repairable += batch.RepairableCount;
            resume = batch.ResumeFrom;
        }
        while (resume is not null);

        return (orphans, repairable);
    }

    private static async Task<T> WaitForAsync<T>(Func<Task<T?>> probe)
        where T : class
    {
        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (true)
        {
            if (await probe() is { } found)
            {
                return found;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("The awaited state did not appear within two minutes.");
            }

            await Task.Delay(500);
        }
    }
}
