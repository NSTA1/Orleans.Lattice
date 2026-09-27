using NSubstitute;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit coverage for <see cref="RepoContextPinnedVectorIndexStore"/>, the
/// decorator that holds every durable-index store operation until the index
/// tree's structural pin has been registered (issue #2829).
/// </summary>
[TestFixture]
public sealed class RepoContextPinnedVectorIndexStoreTests
{
    private const string Prefix = "idx/";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(params string[] keys)
    {
        foreach (var key in keys)
        {
            yield return new KeyValuePair<string, byte[]>(key, [1]);
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static async Task<List<string>> Drain(IAsyncEnumerable<KeyValuePair<string, byte[]>> scan)
    {
        var keys = new List<string>();
        await foreach (var entry in scan)
        {
            keys.Add(entry.Key);
        }

        return keys;
    }

    [Test]
    public void A_null_inner_store_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => _ = new RepoContextPinnedVectorIndexStore(null!, () => Task.CompletedTask));

    [Test]
    public void A_null_pin_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => _ = new RepoContextPinnedVectorIndexStore(Substitute.For<IVectorIndexStore>(), null!));

    [Test]
    public void Once_pinned_every_operation_forwards_the_inner_task_unwrapped()
    {
        var inner = Substitute.For<IVectorIndexStore>();
        var read = Task.FromResult<byte[]?>([7]);
        IReadOnlyDictionary<string, byte[]> many = new Dictionary<string, byte[]>();
        var readMany = Task.FromResult(many);
        var write = Task.FromResult(0);
        var delete = Task.FromResult(1);
        var deletePrefix = Task.FromResult(2);
        var scan = Entries("idx/a");
        inner.ReadAsync("k", Ct).Returns(read);
        inner.ReadManyAsync(Arg.Any<IReadOnlyList<string>>(), Ct).Returns(readMany);
        inner.WriteAsync(Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Ct).Returns(write);
        inner.DeleteAsync(Arg.Any<IReadOnlyList<string>>(), Ct).Returns(delete);
        inner.DeletePrefixAsync(Prefix, Ct).Returns(deletePrefix);
        inner.ScanAsync(Prefix, "idx/0", Ct).Returns(scan);

        var store = new RepoContextPinnedVectorIndexStore(inner, () => Task.CompletedTask);

        // The steady state is every call after the first: it must cost no state
        // machine and no wrapper, so the inner store's own task comes straight back.
        Assert.Multiple(() =>
        {
            Assert.That(store.ReadAsync("k", Ct), Is.SameAs(read));
            Assert.That(store.ReadManyAsync(["k"], Ct), Is.SameAs(readMany));
            Assert.That(store.WriteAsync([new("k", [1])], Ct), Is.SameAs(write));
            Assert.That(store.DeleteAsync(["k"], Ct), Is.SameAs(delete));
            Assert.That(store.DeletePrefixAsync(Prefix, Ct), Is.SameAs(deletePrefix));
            Assert.That(store.ScanAsync(Prefix, "idx/0", Ct), Is.SameAs(scan));
        });
    }

    [Test]
    public async Task The_prefix_only_scan_reaches_the_inner_resumable_scan()
    {
        var inner = Substitute.For<IVectorIndexStore>();
        inner.ScanAsync(Prefix, null, Ct).Returns(Entries("idx/a", "idx/b"));
        var store = new RepoContextPinnedVectorIndexStore(inner, () => Task.CompletedTask);

        var keys = await Drain(store.ScanAsync(Prefix, Ct));

        Assert.That(keys, Is.EqualTo(new[] { "idx/a", "idx/b" }));
        inner.Received(1).ScanAsync(Prefix, null, Ct);
    }

    [Test]
    public async Task While_the_pin_is_pending_every_operation_waits_and_then_forwards()
    {
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = Substitute.For<IVectorIndexStore>();
        inner.ReadAsync("k", Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>([7]));
        inner.ScanAsync(Prefix, null, Arg.Any<CancellationToken>()).Returns(Entries("idx/a"));
        var store = new RepoContextPinnedVectorIndexStore(inner, () => registration.Task);

        var read = store.ReadAsync("k", Ct);
        var readMany = store.ReadManyAsync(["k"], Ct);
        var write = store.WriteAsync([new("k", [1])], Ct);
        var delete = store.DeleteAsync(["k"], Ct);
        var deletePrefix = store.DeletePrefixAsync(Prefix, Ct);
        var scan = Drain(store.ScanAsync(Prefix, Ct));

        Assert.Multiple(() =>
        {
            Assert.That(new Task[] { read, readMany, write, delete, deletePrefix, scan }.Any(t => t.IsCompleted), Is.False);
            Assert.That(inner.ReceivedCalls(), Is.Empty, "nothing may reach the inner store before the pin lands");
        });

        registration.SetResult();

        Assert.That(await read, Is.EqualTo(new byte[] { 7 }));
        await Task.WhenAll(readMany, write, delete, deletePrefix);
        Assert.That(await scan, Is.EqualTo(new[] { "idx/a" }));
        Assert.That(inner.ReceivedCalls().Count(), Is.EqualTo(6));
    }

    [Test]
    public void A_faulted_pin_fails_the_operation_without_touching_the_inner_store()
    {
        var inner = Substitute.For<IVectorIndexStore>();
        var store = new RepoContextPinnedVectorIndexStore(
            inner, () => Task.FromException(new InvalidOperationException("registry unavailable")));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await store.WriteAsync([new("k", [1])], Ct), Throws.InvalidOperationException);
            Assert.That(async () => await Drain(store.ScanAsync(Prefix, Ct)), Throws.InvalidOperationException);
        });
        Assert.That(inner.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Cancellation_while_the_pin_is_pending_abandons_the_wait()
    {
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = Substitute.For<IVectorIndexStore>();
        var store = new RepoContextPinnedVectorIndexStore(inner, () => registration.Task);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            async () => await store.ReadAsync("k", cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(inner.ReceivedCalls(), Is.Empty);
    }
}
