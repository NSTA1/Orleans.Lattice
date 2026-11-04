using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The progress-aware wait's contract (#5724, #5725): it returns once the condition holds, keeps
/// waiting past the stall window while the work advances, fails when the work stops advancing,
/// and fails at the ceiling however steadily the work advances, naming the progress reached.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class ContractWaitSelfTests
{
    private static readonly TimeSpan Stall = TimeSpan.FromSeconds(2);

    /// <summary>A condition that holds returns at once.</summary>
    [Test]
    public async Task A_condition_that_holds_returns()
    {
        await ContractWait.UntilAdvancingAsync("the self-test", () => Task.FromResult(true), () => 0, Stall, TimeSpan.FromMinutes(1), () => "none");
    }

    /// <summary>Work that advances on every poll outlasts the stall window until the condition holds.</summary>
    [Test]
    public async Task Advancing_work_outlasts_the_stall_window()
    {
        var progress = 0L;
        var clock = System.Diagnostics.Stopwatch.StartNew();

        await ContractWait.UntilAdvancingAsync(
            "the self-test",
            () => Task.FromResult(++progress > 0 && clock.Elapsed > Stall * 2),
            () => progress,
            Stall,
            TimeSpan.FromMinutes(1),
            () => progress.ToString(System.Globalization.CultureInfo.InvariantCulture));

        Assert.That(clock.Elapsed, Is.GreaterThan(Stall * 2));
    }

    /// <summary>Work that stops advancing fails after the stall window, naming its progress.</summary>
    [Test]
    public void Stalled_work_fails_and_names_its_progress()
    {
        var ex = Assert.ThrowsAsync<TimeoutException>(() => ContractWait.UntilAdvancingAsync(
            "the self-test", () => Task.FromResult(false), () => 7, Stall, TimeSpan.FromMinutes(1), () => "7 of 9"));

        Assert.That(ex!.Message, Does.Contain("made no progress").And.Contain("7 of 9"));
    }

    /// <summary>Work that keeps advancing still fails at the ceiling.</summary>
    [Test]
    public void Advancing_work_fails_at_the_ceiling()
    {
        var progress = 0L;

        var ex = Assert.ThrowsAsync<TimeoutException>(() => ContractWait.UntilAdvancingAsync(
            "the self-test", () => Task.FromResult(++progress < 0), () => progress, Stall, Stall, () => "advancing"));

        Assert.That(ex!.Message, Does.Contain("ceiling"));
    }

    /// <summary>An operation that keeps completing units outlasts the stall window and its success is returned.</summary>
    [Test]
    public async Task An_advancing_operation_outlasts_the_stall_window_and_succeeds()
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var units = 0L;

        var done = await ContractWait.SucceededWhileAdvancingAsync(
            "the self-test operation",
            () => Task.FromResult<LatticeOperationStatus?>(Status(clock.Elapsed > Stall * 2 ? LatticeOperationState.Succeeded : LatticeOperationState.Running, ++units)),
            Stall,
            TimeSpan.FromMinutes(1));

        Assert.That(done.State, Is.EqualTo(LatticeOperationState.Succeeded));
    }

    /// <summary>An operation that stops advancing times out, naming its trajectory and leaving its last status readable.</summary>
    [Test]
    public void A_stalled_operation_times_out_with_its_last_status()
    {
        var last = new StrongBox<LatticeOperationStatus?>();

        var ex = Assert.ThrowsAsync<TimeoutException>(() => ContractWait.SucceededWhileAdvancingAsync(
            "the self-test operation",
            () => Task.FromResult<LatticeOperationStatus?>(Status(LatticeOperationState.Running, 3)),
            Stall,
            TimeSpan.FromMinutes(1),
            last));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("made no progress").And.Contain("Running/migrating 3"));
            Assert.That(last.Value!.IsTerminal, Is.False);
        });
    }

    /// <summary>An operation not terminal at the deadline times out naming the last status it read.</summary>
    [Test]
    public void A_fixed_deadline_wait_names_the_last_status_on_timeout()
    {
        var ex = Assert.ThrowsAsync<TimeoutException>(() => ContractWait.SucceededAsync(
            "the self-test operation",
            () => Task.FromResult<LatticeOperationStatus?>(Status(LatticeOperationState.Running, 5)),
            TimeSpan.FromSeconds(1)));

        Assert.That(ex!.Message, Does.Contain("Timed out").And.Contain("Last status: Running/migrating 5."));
    }

    private static LatticeOperationStatus Status(LatticeOperationState state, long units) => new()
    {
        OperationId = "op",
        Kind = "self-test",
        Scope = new LatticeOperationScope { TenantId = "default" },
        State = state,
        Phase = "migrating",
        CompletedUnits = units,
    };
}
