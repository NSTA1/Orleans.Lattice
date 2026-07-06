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
    public void Manifest_rejects_a_null_or_empty_set_id_or_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new BackupSetManifest("", "n", DateTimeOffset.UtcNow, false, null, new[] { "m" }),
                Throws.ArgumentException);
            Assert.That(
                () => new BackupSetManifest("id", "", DateTimeOffset.UtcNow, false, null, new[] { "m" }),
                Throws.ArgumentException);
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
}
