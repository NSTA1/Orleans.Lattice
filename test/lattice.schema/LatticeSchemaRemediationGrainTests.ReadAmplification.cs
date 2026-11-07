using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #5802: a slice reads its source a chunk at a time and only as far as it writes,
/// so every key is point-read once per phase however slow the destination is, and each
/// chunk reaches the destination in one batched write. Before the fix a slice read
/// every remaining key up front and wrote only what fitted its time budget, so on a
/// slow destination a tree of N keys cost on the order of N squared point reads.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private const int AmplificationValues = 256;

    private static (string Key, string Value)[] Documents(int count) =>
        Enumerable.Range(0, count).Select(i => ($"k{i:D4}", $"{{\"a\":{i}}}")).ToArray();

    /// <summary>The number of keys the harness's source was asked to point-read.</summary>
    private static int PointReadKeys(Harness h) =>
        h.Source.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(ILattice.GetManyAsync))
            .Sum(call => ((List<string>)call.GetArguments()[0]!).Count);

    private static int BatchedWrites(Harness h) =>
        h.Destination.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILattice.SetManyAsync));

    private static async Task<LatticeSchemaRemediationReport> DriveToTerminalAsync(Harness h, string operationId)
    {
        for (var i = 0; i < 10_000; i++)
        {
            var slice = await h.Grain.RunSliceAsync(operationId);
            if (!slice.Report.InProgress)
            {
                return slice.Report;
            }
        }

        throw new InvalidOperationException("The remediation did not reach a terminal state.");
    }

    [Test]
    public async Task A_slow_destination_point_reads_each_value_once_per_phase()
    {
        var h = CreateGrain(Documents(AmplificationValues));

        // A destination slow enough that a slice spends its budget after a few writes:
        // the regime in which a whole-slice read was re-read on every following slice.
        h.Destination.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.Delay(2));
        h.Grain.SliceTimeBudget = TimeSpan.FromMilliseconds(20);
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-amplification");

        var report = await DriveToTerminalAsync(h, "op-amplification");

        Assert.Multiple(() =>
        {
            Assert.That(report.Succeeded, Is.True, report.Reason);
            Assert.That(report.ScannedCount, Is.EqualTo(AmplificationValues));
            Assert.That(PointReadKeys(h), Is.EqualTo(2 * AmplificationValues),
                "one point read per value in the dry run and one in the build, however many slices the build took");
        });
    }

    [Test]
    public async Task The_build_writes_each_chunk_in_one_batch()
    {
        var h = CreateGrain(Documents(AmplificationValues));
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-batches");

        var report = await DriveToTerminalAsync(h, "op-batches");

        Assert.Multiple(() =>
        {
            Assert.That(report.Succeeded, Is.True, report.Reason);
            Assert.That(BatchedWrites(h), Is.EqualTo(AmplificationValues / LatticeSchemaRemediationGrain.DefaultChunkSize),
                "one batched write per chunk");
        });
        foreach (var (key, _) in Documents(AmplificationValues))
        {
            await h.Destination.Received(1).SetAsync(key, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public async Task A_source_write_that_lands_before_its_chunk_is_read_is_carried()
    {
        var store = Documents(AmplificationValues).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
        var h = CreateGrainCore(
            () => Entries(store.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => (kv.Key, kv.Value)).ToArray()),
            seedState: null,
            schemaRegistry: null,
            existingPolicy: null);

        // A live write to a key far past the build's first chunk, landing once that
        // chunk is written. A slice that read the whole slice up front copied the value
        // it read before the write.
        const string Late = "k0200";
        var written = false;
        h.Destination.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (!written)
                {
                    written = true;
                    store[Late] = "{\"a\":\"live\"}";
                }

                return Task.CompletedTask;
            });
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op-live");

        var report = await DriveToTerminalAsync(h, "op-live");

        Assert.That(report.Succeeded, Is.True, report.Reason);
        await h.Destination.Received(1).SetAsync(
            Late, Arg.Is<byte[]>(v => Text(v) == "{\"a\":\"live\"}"), Arg.Any<CancellationToken>());
    }

    private static string Text(byte[] value) => System.Text.Encoding.UTF8.GetString(value);
}
