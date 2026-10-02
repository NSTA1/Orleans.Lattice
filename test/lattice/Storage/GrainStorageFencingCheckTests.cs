using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Unit tests for <see cref="GrainStorageFencingCheck"/> (issue #4200): how each
/// <see cref="LatticeGrainStorageFencingMode"/> acts on each probe verdict.
/// </summary>
[TestFixture]
public sealed class GrainStorageFencingCheckTests
{
    private static (GrainStorageFencingCheck Check, RecordingLoggerFactory Logs) CreateCheck(
        IGrainStorage? storage,
        LatticeGrainStorageFencingMode mode,
        TimeSpan? timeout = null)
    {
        var services = new ServiceCollection();
        if (storage is not null)
        {
            services.AddKeyedSingleton(LatticeOptions.StorageProviderName, storage);
        }

        var options = Options.Create(new LatticeGrainStorageFencingOptions
        {
            Mode = mode,
            ProbeTimeout = timeout ?? LatticeGrainStorageFencingOptions.DefaultProbeTimeout,
        });
        var logs = new RecordingLoggerFactory();
        var check = new GrainStorageFencingCheck(
            services.BuildServiceProvider(),
            options,
            logs.CreateLogger<GrainStorageFencingCheck>());
        return (check, logs);
    }

    [Test]
    public void RunAsync_reject_mode_unfenced_provider_fails_start()
    {
        var (check, logs) = CreateCheck(new NonFencingGrainStorage(), LatticeGrainStorageFencingMode.Reject);

        var ex = Assert.ThrowsAsync<OrleansConfigurationException>(() => check.RunAsync(CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("stale ETag"));
        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Unfenced));
        Assert.That(logs.Entries.Any(e => e.Level == LogLevel.Error), Is.True);
    }

    [Test]
    public async Task RunAsync_warn_mode_unfenced_provider_logs_a_warning_and_starts()
    {
        var (check, logs) = CreateCheck(new NonFencingGrainStorage(), LatticeGrainStorageFencingMode.Warn);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Unfenced));
        Assert.That(logs.Warnings.Single().Message, Does.Contain("accepted a write carrying a stale ETag"));
    }

    [TestCase(LatticeGrainStorageFencingMode.Warn)]
    [TestCase(LatticeGrainStorageFencingMode.Reject)]
    public async Task RunAsync_fenced_provider_logs_posture_without_warning(LatticeGrainStorageFencingMode mode)
    {
        var (check, logs) = CreateCheck(new FencingGrainStorage(), mode);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced));
        Assert.That(logs.Warnings, Is.Empty);
        var posture = logs.Entries.Single(e => e.Level == LogLevel.Information);
        Assert.That(posture.Value("Verdict"), Is.EqualTo(GrainStorageFencingVerdict.Fenced));
        Assert.That(posture.Value("Mode"), Is.EqualTo(mode));
    }

    [Test]
    public async Task RunAsync_reject_mode_inconclusive_probe_warns_and_starts()
    {
        var storage = new FencingGrainStorage { BeforeWrite = _ => throw new IOException("unreachable") };
        var (check, logs) = CreateCheck(storage, LatticeGrainStorageFencingMode.Reject);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(logs.Warnings.Single().Exception, Is.InstanceOf<IOException>());
    }

    [Test]
    public async Task RunAsync_missing_provider_is_inconclusive()
    {
        var (check, logs) = CreateCheck(null, LatticeGrainStorageFencingMode.Reject);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(logs.Warnings, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task RunAsync_probe_exceeding_the_timeout_is_inconclusive()
    {
        var never = new TaskCompletionSource();
        var storage = new FencingGrainStorage { BeforeWrite = _ => never.Task };
        var (check, _) = CreateCheck(storage, LatticeGrainStorageFencingMode.Reject, TimeSpan.FromMilliseconds(50));

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        Assert.That(check.LastResult.Fault, Is.InstanceOf<TimeoutException>());
    }

    [Test]
    public void Participate_disabled_mode_subscribes_to_nothing()
    {
        var storage = Substitute.For<IGrainStorage>();
        var (check, logs) = CreateCheck(storage, LatticeGrainStorageFencingMode.Disabled);
        var lifecycle = Substitute.For<ISiloLifecycle>();

        check.Participate(lifecycle);

        lifecycle.DidNotReceiveWithAnyArgs().Subscribe(default!, default, default!);
        Assert.That(storage.ReceivedCalls(), Is.Empty);
        Assert.That(check.LastResult, Is.Null);
        Assert.That(logs.Entries.Single().Value("Mode"), Is.EqualTo(LatticeGrainStorageFencingMode.Disabled));
    }

    [TestCase(LatticeGrainStorageFencingMode.Warn)]
    [TestCase(LatticeGrainStorageFencingMode.Reject)]
    public void Participate_enabled_mode_subscribes_at_the_active_stage(LatticeGrainStorageFencingMode mode)
    {
        var (check, _) = CreateCheck(new FencingGrainStorage(), mode);
        var lifecycle = Substitute.For<ISiloLifecycle>();

        check.Participate(lifecycle);

        lifecycle.Received(1).Subscribe(
            nameof(GrainStorageFencingCheck),
            ServiceLifecycleStage.Active,
            Arg.Any<ILifecycleObserver>());
    }
}
