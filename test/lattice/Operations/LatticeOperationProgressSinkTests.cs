using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationProgressSink"/>: which reports are
/// written through and which are coalesced, that progress never regresses, that a
/// grain's stop signal cancels the operation, and that banking writes the last
/// coalesced report on a fault path without masking the fault.
/// </summary>
[TestFixture]
public sealed class LatticeOperationProgressSinkTests
{
    private ILatticeOperationGrain _grain = null!;
    private CancellationTokenSource _cancellation = null!;
    private List<LatticeOperationProgressReport> _written = null!;

    [SetUp]
    public void SetUp()
    {
        _grain = Substitute.For<ILatticeOperationGrain>();
        _written = [];
        _grain.ReportAsync(Arg.Do<LatticeOperationProgressReport>(r => _written.Add(r))).Returns(false);
        _cancellation = new CancellationTokenSource();
    }

    [TearDown]
    public void TearDown() => _cancellation.Dispose();

    private LatticeOperationProgressSink CreateSink() => new(_grain, _cancellation);

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeOperationProgressSink(null!, _cancellation), Throws.ArgumentNullException);
            Assert.That(() => new LatticeOperationProgressSink(_grain, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_first_report_and_every_phase_change_are_written_through()
    {
        var sink = CreateSink();

        await sink.ReportAsync("A");
        await sink.ReportAsync("B", 0, 4, "shards");

        Assert.That(_written.Select(r => r.Phase), Is.EqualTo(new[] { "A", "B" }));
    }

    [Test]
    public async Task Small_steps_are_coalesced_until_one_percent_of_the_total()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, 1000, "entries");

        for (var i = 1; i < 10; i++)
        {
            await sink.ReportAsync("A", i, 1000, "entries");
        }

        Assert.That(_written, Has.Count.EqualTo(1), "Nine units of a thousand stay in memory.");

        await sink.ReportAsync("A", 10, 1000, "entries");

        Assert.That(_written[^1].CompletedUnits, Is.EqualTo(10), "One percent is written through.");
    }

    [Test]
    public async Task The_final_unit_is_always_written_through()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, 1000, "entries");

        await sink.ReportAsync("A", 1000, 1000, "entries");

        Assert.That(_written[^1].CompletedUnits, Is.EqualTo(1000));
    }

    [Test]
    public async Task Without_a_total_every_thousandth_unit_is_written_through()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, null, "entries");

        await sink.ReportAsync("A", 999, null, "entries");
        Assert.That(_written, Has.Count.EqualTo(1));

        await sink.ReportAsync("A", 1000, null, "entries");
        Assert.That(_written, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task A_late_lower_count_never_regresses_the_pending_report()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, 1000, "entries");
        await sink.ReportAsync("A", 5, 1000, "entries");

        await sink.ReportAsync("A", 3, 1000, "entries");
        await sink.FlushAsync();

        Assert.That(_written[^1].CompletedUnits, Is.EqualTo(5));
    }

    [Test]
    public async Task Flush_writes_only_when_something_new_is_pending()
    {
        var sink = CreateSink();
        await sink.FlushAsync();
        Assert.That(_written, Is.Empty);

        await sink.ReportAsync("A");
        await sink.FlushAsync();

        Assert.That(_written, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_stop_signal_cancels_the_operation_and_later_reports_throw()
    {
        _grain.ReportAsync(Arg.Any<LatticeOperationProgressReport>()).Returns(true);
        var sink = CreateSink();

        await sink.ReportAsync("A");

        Assert.Multiple(() =>
        {
            Assert.That(_cancellation.IsCancellationRequested, Is.True);
            Assert.That(async () => await sink.ReportAsync("A", 1), Throws.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public async Task Banking_writes_the_coalesced_report_a_fault_would_otherwise_lose()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, 1000, "entries");
        await sink.ReportAsync("A", 7, 1000, "entries");

        await sink.BankProgressAsync();

        Assert.That(_written[^1].CompletedUnits, Is.EqualTo(7));
    }

    [Test]
    public async Task Banking_swallows_its_own_failure_so_the_original_fault_surfaces()
    {
        var sink = CreateSink();
        await sink.ReportAsync("A", 0, 1000, "entries");
        await sink.ReportAsync("A", 7, 1000, "entries");
        _grain.ReportAsync(Arg.Any<LatticeOperationProgressReport>()).ThrowsAsync(new TimeoutException("grain down"));

        Assert.That(async () => await sink.BankProgressAsync(), Throws.Nothing);
    }

    [Test]
    public void An_empty_phase_is_rejected()
    {
        Assert.That(async () => await CreateSink().ReportAsync(string.Empty), Throws.ArgumentException);
    }
}
