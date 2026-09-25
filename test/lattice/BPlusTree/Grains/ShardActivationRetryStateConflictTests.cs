using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the saga state-write conflict arm of
/// <see cref="ShardActivationRetry"/> (issue #3572): a conflicted saga grain has
/// already deactivated, so retrying lands on a fresh activation that reloads the
/// row. A non-conflict state-write fault is not retried.
/// </summary>
[TestFixture]
public sealed class ShardActivationRetryStateConflictTests
{
    private static LatticeStateWriteFailedException Conflict() =>
        new("atomic-write", "tree/op", new InconsistentStateException("etag"), conflict: true);

    [Test]
    public async Task RunAsync_retries_a_translated_state_write_conflict()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw Conflict();
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public async Task RunAsync_of_T_retries_a_translated_state_write_conflict()
    {
        var calls = 0;
        var result = await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw Conflict();
            return Task.FromResult(42);
        });

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(42));
            Assert.That(calls, Is.EqualTo(2));
        });
    }

    [Test]
    public void RunAsync_does_not_retry_a_non_conflict_state_write_fault()
    {
        var calls = 0;
        Assert.ThrowsAsync<LatticeStateWriteFailedException>(() => ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            throw new LatticeStateWriteFailedException("atomic-write", "tree/op", new IOException("disk"), conflict: false);
        }));

        Assert.That(calls, Is.EqualTo(1));
    }
}
