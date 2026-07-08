namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Test double for <see cref="ISagaCompletionSource"/> that lets a test flip
/// global saga completion on demand. Used to simulate a laggard participant:
/// while <see cref="Complete"/> is <see langword="false"/> the fence primitive
/// must keep shipping paused; flipping it to <see langword="true"/> models every
/// participant having flipped so shipping may resume.
/// </summary>
internal sealed class FakeSagaCompletionSource : ISagaCompletionSource
{
    /// <summary>Whether the saga is reported globally complete.</summary>
    public volatile bool Complete;

    /// <summary>Number of completion probes issued against this source.</summary>
    public int ProbeCount { get; private set; }

    /// <inheritdoc />
    public Task<bool> IsSagaCompleteAsync(
        string sagaId, string coordinatorClusterId, CancellationToken cancellationToken = default)
    {
        ProbeCount++;
        return Task.FromResult(Complete);
    }
}
