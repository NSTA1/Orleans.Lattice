namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupOperationResults"/> and
/// <see cref="BackupOperationScopes"/>: the result maps a tracked backup operation
/// records round-trip to the engine's typed results, and the authorized scopes
/// round-trip through the operation's attributes, failing closed on malformed input.
/// </summary>
[TestFixture]
public sealed class BackupOperationResultsTests
{
    [Test]
    public void A_restore_result_round_trips_through_its_result_map()
    {
        var restore = new LatticeRestoreResult(
            "bk-1", "orders", LatticeRestoreMode.ShadowCutover, "restore-op", ["bk-0", "bk-1"], 42,
            shadowPhysicalTreeId: "phys-new", previousPhysicalTreeId: "phys-old",
            deadLetteredCrossTenant: 2, deadLetteredOverQuota: 3);

        var ok = BackupOperationResults.TryReadRestoreResult(BackupOperationResults.ToResultMap(restore), out var read);

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(read!.BackupId, Is.EqualTo("bk-1"));
            Assert.That(read.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(read.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(read.OperationId, Is.EqualTo("restore-op"));
            Assert.That(read.ManifestChain, Is.EqualTo(new[] { "bk-0", "bk-1" }));
            Assert.That(read.EntriesApplied, Is.EqualTo(42));
            Assert.That(read.ShadowPhysicalTreeId, Is.EqualTo("phys-new"));
            Assert.That(read.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
            Assert.That(read.DeadLetteredCrossTenant, Is.EqualTo(2));
            Assert.That(read.DeadLetteredOverQuota, Is.EqualTo(3));
        });
    }

    [Test]
    public void An_in_place_restore_map_omits_the_shadow_trees()
    {
        var restore = new LatticeRestoreResult("bk-1", "orders", LatticeRestoreMode.InPlace, "op", ["bk-1"], 1);

        var map = BackupOperationResults.ToResultMap(restore);

        Assert.That(map.ContainsKey(BackupOperationResultKeys.ShadowPhysicalTreeId), Is.False);
    }

    [Test]
    public void A_capture_map_is_not_a_restore_result()
    {
        var map = new Dictionary<string, string> { [BackupOperationResultKeys.BackupId] = "bk-1" };

        Assert.That(BackupOperationResults.TryReadRestoreResult(map, out var read), Is.False);
        Assert.That(read, Is.Null);
    }

    [Test]
    public void An_unparseable_mode_is_not_a_restore_result()
    {
        var map = new Dictionary<string, string>
        {
            [BackupOperationResultKeys.BackupId] = "bk",
            [BackupOperationResultKeys.TargetTreeId] = "t",
            [BackupOperationResultKeys.RestoreOperationId] = "op",
            [BackupOperationResultKeys.Mode] = "Sideways",
        };

        Assert.That(BackupOperationResults.TryReadRestoreResult(map, out _), Is.False);
    }

    [Test]
    public void Member_ids_read_back_in_order_and_an_absent_list_is_empty()
    {
        var map = new Dictionary<string, string> { [BackupOperationResultKeys.MemberBackupIds] = "a,b,c" };

        Assert.Multiple(() =>
        {
            Assert.That(BackupOperationResults.ReadMemberBackupIds(map), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(BackupOperationResults.ReadMemberBackupIds(new Dictionary<string, string>()), Is.Empty);
        });
    }

    [Test]
    public void Null_maps_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupOperationResults.TryReadRestoreResult(null!, out _), Throws.ArgumentNullException);
            Assert.That(() => BackupOperationResults.ReadMemberBackupIds(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Scopes_round_trip_through_the_operation_attributes()
    {
        IReadOnlyList<BackupScopeSelector> scopes =
        [
            BackupScopeSelector.WholeTree("a"),
            BackupScopeSelector.Prefix("b", "eu/"),
            BackupScopeSelector.Key("c", "k1"),
        ];

        var attributes = BackupOperationScopes.ToAttributes(scopes);
        var read = BackupOperationScopes.FromOperation(BackupOperationScopes.TreeIds(scopes), attributes);

        Assert.That(read, Is.EqualTo(scopes));
    }

    [Test]
    public void Whole_tree_scopes_need_no_attributes()
    {
        Assert.That(BackupOperationScopes.ToAttributes([BackupScopeSelector.WholeTree("a")]), Is.Empty);
    }

    [Test]
    public void Malformed_scope_attributes_fail_closed()
    {
        var badKind = new Dictionary<string, string> { ["scope.0.kind"] = "Galaxy", ["scope.0.key"] = "x" };
        var missingKey = new Dictionary<string, string> { ["scope.0.kind"] = "Prefix" };

        Assert.Multiple(() =>
        {
            Assert.That(BackupOperationScopes.FromOperation(["a"], badKind), Is.Null);
            Assert.That(BackupOperationScopes.FromOperation(["a"], missingKey), Is.Null);
            Assert.That(BackupOperationScopes.FromOperation([], new Dictionary<string, string>()), Is.Null,
                "An operation with no trees authorizes against nothing, so it is never visible.");
        });
    }
}
