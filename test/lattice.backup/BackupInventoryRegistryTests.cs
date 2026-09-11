namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupInventoryRegistry"/> that exercise every
/// uncovered path: the <c>BackupRecord</c> struct
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

        // BytesReclaimed must reflect the manifest's descriptor byte lengths. The
        // expectation is summed from the manifest rather than hard-coded so the
        // assertion stays exact if the shared sample gains another descriptor,
        // and it still fails if the else branch stops adding anything at all.
        var expected = manifest.ContentDescriptors.Sum(d => d.ByteLength);
        Assert.That(expected, Is.GreaterThan(0),
            "the sample manifest must carry a non-zero descriptor or this test cannot distinguish "
            + "the else branch from a no-op");
        Assert.That(_registry.BytesReclaimed, Is.EqualTo(expected));
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

    // ---- EnsureScopeRegistered (issue #2645) ---------------------------------------------

    [Test]
    public void EnsureScopeRegistered_makes_a_never_run_scope_enumerable_as_none()
    {
        // Before #2645 RecordScopeOutcome was the only insertion point, so a scope
        // with a schedule but no completed cycle was absent from EnumerateScopes
        // and BackupScopeRunOutcome.None was unreachable on the status gauge.
        _registry.EnsureScopeRegistered("scheduled-scope");

        var scopes = _registry.EnumerateScopes();

        Assert.That(scopes.Select(p => p.Key), Does.Contain("scheduled-scope"));
        var runtime = _registry.TryGetScope("scheduled-scope");
        Assert.That(runtime, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(runtime!.Value.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.None));
            Assert.That(runtime.Value.LastRunUtc, Is.Null, "registration is not a run");
            Assert.That(runtime.Value.LastSuccessUtc, Is.Null, "registration is not a success");
        });
    }

    [Test]
    public void EnsureScopeRegistered_does_not_clobber_an_already_recorded_outcome()
    {
        // A schedule is re-registered on every EnsureScheduleAsync, so a
        // destructive registration would reset a recorded failure to None and
        // report a failing scope as merely pending.
        var ranAt = DateTimeOffset.UtcNow;
        _registry.RecordScopeOutcome("busy-scope", BackupScopeRunOutcome.Failure, ranAt);

        _registry.EnsureScopeRegistered("busy-scope");

        var runtime = _registry.TryGetScope("busy-scope");
        Assert.That(runtime, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(runtime!.Value.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Failure));
            Assert.That(runtime.Value.LastRunUtc, Is.EqualTo(ranAt));
        });
    }

    [Test]
    public void EnsureScopeRegistered_is_idempotent_and_adds_one_entry()
    {
        _registry.EnsureScopeRegistered("repeat-scope");
        _registry.EnsureScopeRegistered("repeat-scope");

        Assert.That(_registry.EnumerateScopes().Count(p => p.Key == "repeat-scope"), Is.EqualTo(1));
    }

    [Test]
    public void EnsureScopeRegistered_with_null_or_empty_scope_key_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => _registry.EnsureScopeRegistered(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => _registry.EnsureScopeRegistered(""), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void EnumerateScopes_is_empty_for_a_registry_with_no_registered_or_run_scope()
    {
        // Pins the other half of the reading: absence means "not scheduled".
        Assert.That(_registry.EnumerateScopes(), Is.Empty);
    }
}
