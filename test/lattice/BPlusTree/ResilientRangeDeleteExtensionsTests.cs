using NSubstitute;
using NSubstitute.Core;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the resilient range-delete drain wrapper
/// (<see cref="LatticeExtensions.DeleteRangeAsync(ILattice, string, string, int, int?, System.Threading.CancellationToken)"/>),
/// the delete-side analogue of <see cref="LatticeExtensions.ScanKeysAsync"/>.
/// </summary>
public class ResilientRangeDeleteExtensionsTests
{
    private static LatticeCursorDeleteProgress Progress(int deletedThisStep, bool isComplete) =>
        new() { DeletedThisStep = deletedThisStep, DeletedTotal = 0, IsComplete = isComplete };

    private static ILattice StubLattice(
        Func<CallInfo, Task<LatticeCursorDeleteProgress>> stepProducer,
        Action<int>? onOpen = null)
    {
        var lattice = Substitute.For<ILattice>();
        var opens = 0;
        lattice.OpenDeleteRangeCursorAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var idx = opens++;
                onOpen?.Invoke(idx);
                return Task.FromResult($"cursor-{idx}");
            });
        lattice.DeleteRangeStepAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(stepProducer);
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        return lattice;
    }

    [Test]
    public async Task DeleteRangeAsync_drains_all_steps_and_returns_total_when_no_abort()
    {
        var steps = new Queue<LatticeCursorDeleteProgress>(new[]
        {
            Progress(100, false),
            Progress(100, false),
            Progress(37, true),
        });
        var lattice = StubLattice(_ => Task.FromResult(steps.Dequeue()));

        var total = await lattice.DeleteRangeAsync("repo/x/", "repo/x0", stepSize: 100);

        Assert.That(total, Is.EqualTo(237));
        await lattice.Received(1).OpenDeleteRangeCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await lattice.Received(1).CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRangeAsync_reopens_a_fresh_cursor_after_a_single_abort_and_completes()
    {
        // First step on the first cursor aborts; after reopen the fresh cursor
        // drains what remains (already-tombstoned keys are gone, so the resumed
        // cursor only reports keys it actually deletes).
        var callIndex = 0;
        var lattice = StubLattice(_ =>
        {
            var idx = callIndex++;
            return idx switch
            {
                0 => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
                _ => Task.FromResult(Progress(42, true)),
            };
        });

        var total = await lattice.DeleteRangeAsync("a", "b", stepSize: 256);

        Assert.That(total, Is.EqualTo(42));
        await lattice.Received(2).OpenDeleteRangeCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        // Both the abandoned and the completed cursor are closed (best-effort).
        await lattice.Received(2).CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRangeAsync_accumulates_across_multiple_reopens()
    {
        var callIndex = 0;
        var lattice = StubLattice(_ =>
        {
            var idx = callIndex++;
            return idx switch
            {
                0 => Task.FromResult(Progress(10, false)),
                1 => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
                2 => Task.FromResult(Progress(5, false)),
                3 => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
                _ => Task.FromResult(Progress(3, true)),
            };
        });

        var total = await lattice.DeleteRangeAsync("a", "z", stepSize: 64);

        Assert.That(total, Is.EqualTo(18));
        await lattice.Received(3).OpenDeleteRangeCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void DeleteRangeAsync_rethrows_after_budget_exhausted()
    {
        var opens = 0;
        var lattice = StubLattice(
            _ => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
            onOpen: _ => opens++);

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100, maxAttempts: 2));

        // Initial open + one reopen per attempt within budget = 1 + 2 = 3.
        Assert.That(opens, Is.EqualTo(3));
    }

    [Test]
    public void DeleteRangeAsync_maxAttempts_zero_fails_on_first_abort_without_reopening()
    {
        var opens = 0;
        var lattice = StubLattice(
            _ => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
            onOpen: _ => opens++);

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100, maxAttempts: 0));

        Assert.That(opens, Is.EqualTo(1), "maxAttempts=0 must not reopen the cursor.");
    }

    [Test]
    public void DeleteRangeAsync_negative_maxAttempts_is_clamped_to_zero()
    {
        var opens = 0;
        var lattice = StubLattice(
            _ => Task.FromException<LatticeCursorDeleteProgress>(new EnumerationAbortedException()),
            onOpen: _ => opens++);

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100, maxAttempts: -5));

        Assert.That(opens, Is.EqualTo(1), "Negative budget clamps to zero - no reconnects.");
    }

    [Test]
    public void DeleteRangeAsync_propagates_non_abort_exceptions_immediately()
    {
        var lattice = StubLattice(
            _ => Task.FromException<LatticeCursorDeleteProgress>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100));
    }

    [Test]
    public async Task DeleteRangeAsync_closes_cursor_even_when_a_non_abort_exception_is_thrown()
    {
        var lattice = StubLattice(
            _ => Task.FromException<LatticeCursorDeleteProgress>(new InvalidOperationException("boom")));

        try
        {
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100);
        }
        catch (InvalidOperationException)
        {
            // expected
        }

        await lattice.Received(1).CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void DeleteRangeAsync_honors_cancellation_token()
    {
        var lattice = StubLattice(_ => Task.FromResult(Progress(1, false)));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 100, cancellationToken: cts.Token));
    }

    [Test]
    public void DeleteRangeAsync_throws_for_null_lattice()
    {
        ILattice? lattice = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await lattice!.DeleteRangeAsync("a", "b", stepSize: 100));
    }

    [Test]
    public void DeleteRangeAsync_throws_for_null_bounds()
    {
        var lattice = StubLattice(_ => Task.FromResult(Progress(0, true)));

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await lattice.DeleteRangeAsync(null!, "b", stepSize: 100));
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await lattice.DeleteRangeAsync("a", null!, stepSize: 100));
    }

    [Test]
    public void DeleteRangeAsync_throws_for_nonpositive_step_size()
    {
        var lattice = StubLattice(_ => Task.FromResult(Progress(0, true)));

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: 0));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await lattice.DeleteRangeAsync("a", "b", stepSize: -1));
    }
}
