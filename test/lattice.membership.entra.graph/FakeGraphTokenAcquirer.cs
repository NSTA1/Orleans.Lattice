namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// A fake <see cref="IEntraGraphTokenAcquirer"/> that counts acquisitions and can
/// block the next one so a test can pile up concurrent callers and observe the
/// single-flight behaviour. Each token it issues is distinct and expires a fixed
/// lifetime after the current clock reading.
/// </summary>
internal sealed class FakeGraphTokenAcquirer(TimeProvider clock, TimeSpan lifetime) : IEntraGraphTokenAcquirer
{
    private TaskCompletionSource? _gate;
    private int _callCount;

    /// <summary>The number of times <see cref="AcquireAsync"/> was invoked.</summary>
    public int CallCount => Volatile.Read(ref _callCount);

    /// <summary>Arms a gate so the next acquisition blocks until <see cref="Release"/> is called.</summary>
    public void BlockNext() =>
        _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Releases a gate armed by <see cref="BlockNext"/>.</summary>
    public void Release() => _gate?.TrySetResult();

    /// <inheritdoc />
    public async Task<EntraGraphToken> AcquireAsync(CancellationToken cancellationToken)
    {
        var count = Interlocked.Increment(ref _callCount);
        var gate = _gate;
        if (gate is not null)
        {
            await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return new EntraGraphToken($"token-{count}", clock.GetUtcNow() + lifetime);
    }
}
