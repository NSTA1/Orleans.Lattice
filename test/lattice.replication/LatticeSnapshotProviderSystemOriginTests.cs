using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the system-origin wrapper the snapshot export streams through
/// (issue #5683): every step of the source runs under a system-origin scope, the
/// consumer between steps does not, and entering that scope costs nothing per row
/// (issue #5713).
/// </summary>
[TestFixture]
public class LatticeSnapshotProviderSystemOriginTests
{
    [Test]
    public async Task Every_step_and_the_dispose_run_under_system_origin_and_the_consumer_does_not()
    {
        var source = new ProbeSource(rows: 4, yieldBeforeEachRow: false);

        var seen = 0;
        await foreach (var _ in LatticeSnapshotProvider.InSystemOriginAsync(source))
        {
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the consumer must not inherit the export's scope");
            seen++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(4));
            Assert.That(source.Steps, Is.EqualTo(5));
            Assert.That(source.StepsOutsideSystemOrigin, Is.Zero);
            Assert.That(source.DisposedUnderSystemOrigin, Is.True);
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
        });
    }

    [Test]
    public async Task A_step_that_resumes_after_an_await_still_runs_under_system_origin()
    {
        var source = new ProbeSource(rows: 3, yieldBeforeEachRow: true);

        var seen = 0;
        await foreach (var _ in LatticeSnapshotProvider.InSystemOriginAsync(source))
        {
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
            seen++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(3));
            Assert.That(source.StepsOutsideSystemOrigin, Is.Zero);
            Assert.That(source.DisposedUnderSystemOrigin, Is.True);
        });
    }

    [Test]
    public void Entering_the_system_origin_scope_allocates_nothing_per_row()
    {
        var growth = AllocationProbe.Growth(
            static rows => new ProbeSource(rows, yieldBeforeEachRow: false),
            static (source, _) =>
            {
                var enumerator = LatticeSnapshotProvider.InSystemOriginAsync(source).GetAsyncEnumerator();
                long count = 0;
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    count++;
                }

                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                AllocationProbe.ScalarSink += count;
            },
            smallSize: 16,
            largeSize: 1_016);

        Assert.That(growth, Is.Zero, "the per-row system-origin scope must not allocate");
    }

    /// <summary>
    /// A source that records whether each step ran under system origin. Its
    /// steps complete synchronously unless asked to yield first.
    /// </summary>
    private sealed class ProbeSource(int rows, bool yieldBeforeEachRow) : IAsyncEnumerable<SnapshotEntry>, IAsyncEnumerator<SnapshotEntry>
    {
        private int _remaining = rows;

        public int Steps { get; private set; }

        public int StepsOutsideSystemOrigin { get; private set; }

        public bool DisposedUnderSystemOrigin { get; private set; }

        public SnapshotEntry Current => default;

        public IAsyncEnumerator<SnapshotEntry> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;

        public ValueTask<bool> MoveNextAsync() =>
            yieldBeforeEachRow ? new ValueTask<bool>(YieldThenStepAsync()) : new ValueTask<bool>(Step());

        public ValueTask DisposeAsync()
        {
            DisposedUnderSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
            return ValueTask.CompletedTask;
        }

        private async Task<bool> YieldThenStepAsync()
        {
            await Task.Yield();
            return Step();
        }

        private bool Step()
        {
            Steps++;
            if (!LatticeAccessGateContext.IsSystemOrigin)
            {
                StepsOutsideSystemOrigin++;
            }

            return _remaining-- > 0;
        }
    }
}
