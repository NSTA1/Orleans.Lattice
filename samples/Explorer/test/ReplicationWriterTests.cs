using System.Reflection;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class ReplicationWriterTests
{
    [Test]
    public void Keys_cycle_through_the_fixed_key_set()
    {
        var target = new ReplicationWriterTarget("east", RecordingLattice.Create(out _), "machine-", 3);

        var keys = Enumerable.Range(0, 7).Select(tick => ReplicationWriter.KeyFor(target, tick)).ToArray();

        Assert.That(keys, Is.EqualTo(new[] { "machine-000", "machine-001", "machine-002", "machine-000", "machine-001", "machine-002", "machine-000" }));
        Assert.That(keys.Distinct().Count(), Is.EqualTo(target.KeyCount), "the writer never writes more keys than its key set");
    }

    [Test]
    public async Task Each_tick_writes_one_key_per_target_and_stays_within_each_key_set()
    {
        var east = RecordingLattice.Create(out var eastWrites);
        var west = RecordingLattice.Create(out var westWrites);
        await using var writer = new ReplicationWriter(
            [new ReplicationWriterTarget("east", east, "m-", 2), new ReplicationWriterTarget("west", west, "w-", 1)],
            TimeSpan.FromSeconds(1));

        for (var i = 0; i < 5; i++)
        {
            await writer.WriteOnceAsync();
        }

        Assert.That(writer.Ticks, Is.EqualTo(5));
        Assert.That(eastWrites.Select(write => write.Key), Is.EqualTo(new[] { "m-000", "m-001", "m-000", "m-001", "m-000" }));
        Assert.That(westWrites.Select(write => write.Key), Is.EqualTo(new[] { "w-000", "w-000", "w-000", "w-000", "w-000" }));
        Assert.That(eastWrites.Select(write => write.Value), Is.EqualTo(new[] { "status-0", "status-1", "status-2", "status-3", "status-4" }));
        Assert.That(eastWrites.All(write => write.SystemOrigin), Is.True, "the writer is trusted co-hosted infrastructure");
    }

    [Test]
    public async Task Disposing_a_writer_that_never_started_completes()
    {
        var writer = new ReplicationWriter([], TimeSpan.FromSeconds(1));

        await writer.DisposeAsync();

        Assert.That(writer.Ticks, Is.Zero);
    }

    [Test]
    public void Invalid_arguments_throw()
    {
        var target = new ReplicationWriterTarget("east", RecordingLattice.Create(out _), "k-", 1);

        Assert.That(() => new ReplicationWriter(null!, TimeSpan.FromSeconds(1)), Throws.ArgumentNullException);
        Assert.That(() => new ReplicationWriter([], TimeSpan.Zero), Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(() => ReplicationWriter.KeyFor(null!, 0), Throws.ArgumentNullException);
        Assert.That(() => ReplicationWriter.KeyFor(target, -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-3)]
    public void A_target_needs_at_least_one_key(int keyCount) =>
        Assert.That(
            () => new ReplicationWriterTarget("east", RecordingLattice.Create(out _), "k-", keyCount),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    /// <summary>An <see cref="ILattice"/> that records <c>SetAsync(key, value, token)</c> and supports nothing else.</summary>
    public class RecordingLattice : DispatchProxy
    {
        private List<(string Key, string Value, bool SystemOrigin)> _writes = null!;

        public static ILattice Create(out List<(string Key, string Value, bool SystemOrigin)> writes)
        {
            var proxy = Create<ILattice, RecordingLattice>();
            writes = [];
            ((RecordingLattice)(object)proxy)._writes = writes;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is { Name: nameof(ILattice.SetAsync) } && args is [string key, byte[] value, ..] && targetMethod.ReturnType == typeof(Task))
            {
                _writes.Add((key, System.Text.Encoding.UTF8.GetString(value), LatticeSystemOrigin.IsActive));
                return Task.CompletedTask;
            }

            throw new NotSupportedException($"{targetMethod?.Name} is not recorded.");
        }
    }
}
