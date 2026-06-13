using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the default <see cref="IReplicationDigestProbeTransport.PullCompressionDictionaryAsync"/>
/// implementation and the <see cref="AutoSharedDictionaryTrainingService"/>
/// background training pump.
/// </summary>
[TestFixture]
public class AutoSharedDictionaryTrainingServiceTests
{
    private sealed class BareTransport : IReplicationDigestProbeTransport
    {
        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)
            => Task.FromResult(default(DigestProbeResponse));
    }

    private sealed class FakeClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        private readonly List<(DateTimeOffset Due, ManualTimer Timer)> _timers = new();

        public override DateTimeOffset GetUtcNow() => _now;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(callback, state);
            lock (_timers)
            {
                _timers.Add((_now + dueTime, timer));
            }
            return timer;
        }

        public void Advance(TimeSpan delta)
        {
            _now += delta;
            (DateTimeOffset Due, ManualTimer Timer)[] due;
            lock (_timers)
            {
                due = _timers.Where(t => t.Due <= _now).ToArray();
                _timers.RemoveAll(t => t.Due <= _now);
            }

            foreach (var t in due)
            {
                t.Timer.Fire();
            }
        }

        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            public void Fire() => callback(state);
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() { }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Default_pull_transport_reports_not_supported()
    {
        var response = await ((IReplicationDigestProbeTransport)new BareTransport()).PullCompressionDictionaryAsync(
            "peer", new CompressionDictionaryPullRequest { DictionaryId = 3u }, default);

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.False);
            Assert.That(response.Found, Is.False);
        });
    }

    [Test]
    public async Task Pump_does_nothing_when_training_is_disabled()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = false });
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        var service = new AutoSharedDictionaryTrainingService(
            provider, NullLogger<AutoSharedDictionaryTrainingService>.Instance, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        clock.Advance(TimeSpan.FromMinutes(5));
        await cts.CancelAsync();
        await service.StopAsync(default);

        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(0u));
    }

    [Test]
    public async Task Pump_trains_a_dictionary_when_the_clock_advances()
    {
        var options = new CompressionDictionaryTrainingOptions
        {
            Enabled = true,
            MinSamplesToTrain = 8,
            MinTrainingInterval = TimeSpan.FromSeconds(1),
            DictionaryCapacityBytes = 4096,
            MaxSampleCount = 4096,
            MaxReservoirBytes = 16L * 1024 * 1024,
        };
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(options, clock);

        for (var i = 0; i < 64; i++)
        {
            provider.Observe(System.Text.Encoding.UTF8.GetBytes(
                $"user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|"));
        }

        var service = new AutoSharedDictionaryTrainingService(
            provider, NullLogger<AutoSharedDictionaryTrainingService>.Instance, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);

        // Advance past the poll interval so the pump's Task.Delay completes and
        // a training pass runs.
        for (var i = 0; i < 4 && provider.CurrentDictionaryId == 0u; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            await Task.Delay(50);
        }

        await cts.CancelAsync();
        await service.StopAsync(default);

        Assert.That(provider.CurrentDictionaryId, Is.Not.EqualTo(0u));
    }

    [Test]
    public void Constructor_throws_on_null_arguments()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = true });

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new AutoSharedDictionaryTrainingService(
                    null!, NullLogger<AutoSharedDictionaryTrainingService>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new AutoSharedDictionaryTrainingService(provider, null!),
                Throws.ArgumentNullException);
        });
    }
}
