namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Argument-validation coverage for the cross-tree backup-set surface:
/// <see cref="LatticeBackupSetCaptureRequest"/>, <see cref="LatticeBackupSetCaptureResult"/>,
/// <see cref="BackupSetManifest"/>, and <see cref="BackupSetFence"/>. These pure
/// value types guard their inputs at construction, independent of any cluster.
/// </summary>
public sealed class BackupSetModelTests
{
    private static BackupScopeSelector Tree(string id) => BackupScopeSelector.WholeTree(id);

    // ---- LatticeBackupSetCaptureRequest ---------------------------------

    [Test]
    public void Request_rejects_a_null_or_empty_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeBackupSetCaptureRequest(null!, new[] { Tree("a") }),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => new LatticeBackupSetCaptureRequest("", new[] { Tree("a") }),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Request_rejects_null_scopes()
    {
        Assert.That(
            () => new LatticeBackupSetCaptureRequest("set", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Request_rejects_empty_scopes()
    {
        Assert.That(
            () => new LatticeBackupSetCaptureRequest("set", Array.Empty<BackupScopeSelector>()),
            Throws.ArgumentException);
    }

    [Test]
    public void Request_rejects_two_scopes_naming_the_same_tree()
    {
        Assert.That(
            () => new LatticeBackupSetCaptureRequest("set", new[] { Tree("a"), Tree("a") }),
            Throws.ArgumentException);
    }

    [Test]
    public void Request_rejects_a_non_positive_page_size()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeBackupSetCaptureRequest("set", new[] { Tree("a") }, pageSize: 0),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => new LatticeBackupSetCaptureRequest("set", new[] { Tree("a") }, pageSize: -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Request_defaults_cross_tree_consistent_to_false()
    {
        var request = new LatticeBackupSetCaptureRequest("set", new[] { Tree("a"), Tree("b") });

        Assert.Multiple(() =>
        {
            Assert.That(request.CrossTreeConsistent, Is.False);
            Assert.That(request.Scopes, Has.Count.EqualTo(2));
            Assert.That(request.Name, Is.EqualTo("set"));
        });
    }

    // ---- BackupSetFence -------------------------------------------------

    [Test]
    public void Fence_rejects_negative_measurements()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new BackupSetFence(-1, 0, 0, 1), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new BackupSetFence(0, -1, 0, 1), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new BackupSetFence(0, 0, -1, 1), Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Fence_rejects_a_non_positive_attempt_count()
    {
        Assert.That(() => new BackupSetFence(0, 0, 0, 0), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Fence_round_trips_its_measurements()
    {
        var fence = new BackupSetFence(123L, 2, 45.5, 3);

        Assert.Multiple(() =>
        {
            Assert.That(fence.HlcTimestamp, Is.EqualTo(123L));
            Assert.That(fence.DrainedInFlightCount, Is.EqualTo(2));
            Assert.That(fence.DrainWaitMilliseconds, Is.EqualTo(45.5));
            Assert.That(fence.Attempts, Is.EqualTo(3));
        });
    }

    // ---- BackupSetManifest ----------------------------------------------

    [Test]
    public void Manifest_rejects_an_empty_set_id_or_a_null_or_empty_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new BackupSetManifest("", "n", DateTimeOffset.UtcNow, false, null, new[] { "m" }),
                Throws.ArgumentException,
                "an empty id is a malformed id, not the absence of one");
            Assert.That(
                () => new BackupSetManifest("id", "", DateTimeOffset.UtcNow, false, null, new[] { "m" }),
                Throws.ArgumentException);
            Assert.That(
                () => new BackupSetManifest("id", null!, DateTimeOffset.UtcNow, false, null, new[] { "m" }),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Manifest_accepts_a_null_set_id_for_an_unidentified_set()
    {
        // A single-member set records no durable membership, so it carries no set
        // id at all rather than one that resolves to nothing.
        var manifest = new BackupSetManifest(
            null, "solo", DateTimeOffset.UnixEpoch, false, null, new[] { "m1" });

        Assert.Multiple(() =>
        {
            Assert.That(manifest.SetId, Is.Null);
            Assert.That(manifest.Name, Is.EqualTo("solo"));
            Assert.That(manifest.MemberBackupIds, Is.EqualTo(new[] { "m1" }));
        });
    }

    [Test]
    public void Manifest_rejects_null_or_empty_members()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new BackupSetManifest("id", "n", DateTimeOffset.UtcNow, false, null, null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => new BackupSetManifest("id", "n", DateTimeOffset.UtcNow, false, null, Array.Empty<string>()),
                Throws.ArgumentException);
        });
    }

    [Test]
    public void Manifest_round_trips_a_cross_tree_consistent_set()
    {
        var fence = new BackupSetFence(7L, 1, 12.0, 2);
        var manifest = new BackupSetManifest(
            "set-id", "nightly", DateTimeOffset.UnixEpoch, true, fence, new[] { "m1", "m2" });

        Assert.Multiple(() =>
        {
            Assert.That(manifest.CrossTreeConsistent, Is.True);
            Assert.That(manifest.Fence, Is.EqualTo(fence));
            Assert.That(manifest.MemberBackupIds, Is.EqualTo(new[] { "m1", "m2" }));
        });
    }

    // ---- LatticeBackupSetCaptureResult ----------------------------------

    [Test]
    public void Result_rejects_a_null_set_manifest()
    {
        Assert.That(
            () => new LatticeBackupSetCaptureResult(null!, Array.Empty<LatticeBackupCaptureResult>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Result_rejects_an_empty_members_list()
    {
        // Line 24: the members-empty guard.
        var setManifest = new BackupSetManifest(null, "nightly", DateTimeOffset.UnixEpoch, false, null, new[] { "m1" });
        Assert.That(
            () => new LatticeBackupSetCaptureResult(setManifest, Array.Empty<LatticeBackupCaptureResult>()),
            Throws.ArgumentException.With.Message.Contains("at least one member"));
    }

    // ---- BackupRetentionReport ------------------------------------------

    [Test]
    public void BackupRetentionReport_Empty_has_zero_retained_and_zero_pruned()
    {
        // Line 41: the Empty static-property getter.
        var empty = BackupRetentionReport.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(empty.PrunedCount, Is.Zero);
            Assert.That(empty.RetainedCount, Is.Zero);
            Assert.That(empty.PrunedBackupIds, Is.Empty);
        });
    }

    // ---- LatticeRestoreRequest ------------------------------------------

    [Test]
    public void LatticeRestoreRequest_rejects_empty_operation_id()
    {
        // Line 62: when operationId is non-null but empty, ArgumentException is thrown.
        Assert.That(
            () => new LatticeRestoreRequest("backup-id", operationId: ""),
            Throws.InstanceOf<ArgumentException>());
    }
}
