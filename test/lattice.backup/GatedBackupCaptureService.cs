namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An <see cref="ILatticeBackupCaptureService"/> test double that gates a capture
/// mid-flight: it signals when a capture has entered (so the caller knows the
/// scheduler's overlap guard is now armed), blocks until the test releases it,
/// then delegates to the real capture engine to produce a genuine backup. It
/// counts how many captures actually started, so a test can assert that an
/// overlapping trigger was skipped rather than executed.
/// </summary>
internal sealed class GatedBackupCaptureService(LatticeBackupCaptureService inner)
    : ILatticeBackupCaptureService
{
    private readonly TaskCompletionSource _started =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource _release =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _calls;

    /// <summary>Completes once a capture has entered the gate.</summary>
    public Task Started => _started.Task;

    /// <summary>The number of captures that actually started.</summary>
    public int Calls => Volatile.Read(ref _calls);

    /// <summary>Releases the gated capture so it can complete.</summary>
    public void Release() => _release.TrySetResult();

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CaptureAsync(
        LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _calls);
        _started.TrySetResult();
        await _release.Task;
        return await inner.CaptureAsync(request, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeBackupSetCaptureResult> CaptureSetAsync(
        LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default) =>
        inner.CaptureSetAsync(request, cancellationToken);
}
