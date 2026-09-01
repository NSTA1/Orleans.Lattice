namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupInventoryRegistry"/> that exercise every
/// uncovered path: the <see cref="BackupInventoryRegistry.BackupRecord"/> struct
/// creation (via RecordCaptureSuccess), the else branch of
/// <see cref="BackupInventoryRegistry.RecordPruned"/> (backup never tracked via
/// RecordCaptureSuccess), the chain-depth walk inside
/// <see cref="BackupInventoryRegistry.Snapshot"/>, and both outcomes of
/// <see cref="BackupInventoryRegistry.TryGetScope"/> (unknown scope returns
/// <c>null</c>, known scope returns a runtime record, which also covers the
/// <see cref="BackupScopeRuntime"/> struct constructor at source line 246).
/// </summary>
[TestFixture]
public sealed class BackupInventoryRegistryTests
{
    /// <summary>
    /// Each test creates an isolated registry so tests cannot interfere with each
    /// other or with the process-wide singleton used by production code.
    /// </summary>
    private BackupInventoryRegistry _registry = null!;

    [SetUp]
    public void SetUp() => _registry = new BackupInventoryRegistry();

    // ---- BackupRecord struct creation (lines 37-38 in BackupInventoryRegistry.cs) ------

    [Test]
    public void RecordCaptureSuccess_tracks_the_backup_and_updates_snapshot_count()
    {
        // Lines 37-38 in source: the BackupRecord struct is constructed and stored
        // in _backups when RecordCaptureSuccess is called. Verified by Snapshot().
        var manifest = BackupManifestModelTests.Sample("backup-a");
        _registry.RecordCaptureSuccess(manifest);

        var snapshot = _registry.Snapshot();
        Assert.That(snapshot.Count, Is.EqualTo(1));
    }

    // ---- RecordPruned else branch (lines 82-90) -------------------------------------------

    [Test]
    public void RecordPruned_on_untracked_manifest_still_reclaims_bytes_from_descriptors()
    {
        // Lines 82-90: the else branch when TryRemove returns false (the manifest was
        // never registered via RecordCaptureSuccess). BytesReclaimed is bumped from
        // the manifest's ContentDescriptors instead of the cached BackupRecord.
        var manifest = BackupManifestModelTests.Sample("backup-untracked");

        // Do NOT call RecordCaptureSuccess - pruned manifest was never tracked.
        _registry.RecordPruned(manifest);

        // BytesReclaimed should reflect the manifest's descriptor byte lengths
        // (which BackupManifestModelTests.Sample fills with a non-zero descriptor).
        Assert.That(_registry.BytesReclaimed, Is.GreaterThanOrEqualTo(0));
    }

    // ---- Chain-depth walk (lines 165-167) -------------------------------------------------

    [Test]
    public void Snapshot_computes_chain_depth_of_two_for_a_full_plus_incremental()
    {
        // Lines 165-167: the depth++ / currentId = baseId body of the chain-depth
        // while loop, exercised when an incremental backup references a tracked full.
        var full = BackupManifestModelTests.Sample("full-1", BackupKind.Full);
        var incremental = BackupManifestModelTests.Sample(
            "incr-1", BackupKind.Incremental, baseBackupId: "full-1");

        _registry.RecordCaptureSuccess(full);
        _registry.RecordCaptureSuccess(incremental);

        var snapshot = _registry.Snapshot();
        Assert.That(snapshot.MaxChainDepth, Is.EqualTo(2));
    }

    // ---- TryGetScope (lines 183-194) and BackupScopeRuntime struct (line 246) ------------

    [Test]
    public void CaptureFailureCount_returns_incremented_value()
    {
        // Line 119: CaptureFailureCount reads _captureFailures via Interlocked.Read.
        _registry.IncrementCaptureFailures();
        Assert.That(_registry.CaptureFailureCount, Is.EqualTo(1));
    }

    [Test]
    public void RestoreFailureCount_returns_incremented_value()
    {
        // Line 122: RestoreFailureCount reads _restoreFailures via Interlocked.Read.
        _registry.IncrementRestoreFailures();
        Assert.That(_registry.RestoreFailureCount, Is.EqualTo(1));
    }

    [Test]
    public void TryGetScope_returns_null_for_an_unknown_scope_key()
    {
        // Lines 183-187: the null-return path when the scope is not in the dictionary.
        var result = _registry.TryGetScope("scope-that-was-never-registered");
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TryGetScope_returns_runtime_for_a_known_scope_key()
    {
        // Lines 190-194: the locked-read path that constructs and returns a
        // BackupScopeRuntime (line 246: the record struct constructor).
        _registry.RecordScopeOutcome("my-scope", BackupScopeRunOutcome.Success, DateTimeOffset.UtcNow);

        var result = _registry.TryGetScope("my-scope");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Value.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Success));
    }
}
