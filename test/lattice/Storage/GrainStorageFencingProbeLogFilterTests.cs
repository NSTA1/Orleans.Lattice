using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Tests for <see cref="GrainStorageFencingProbeLogFilter"/> (issue #5442): a
/// healthy silo start logs no Error from the fencing probe's deliberate stale
/// write, the host's logging configuration is otherwise untouched, and a real
/// fencing failure or provider fault is still reported.
/// </summary>
[TestFixture]
public sealed class GrainStorageFencingProbeLogFilterTests
{
    private const string Category = "Orleans.Storage.SomeGrainStorage";

    [Test]
    public void Pipeline_outside_a_probe_keeps_errors()
    {
        var (services, records) = BuildLogging();
        using var _ = services;

        services.GetRequiredService<ILoggerFactory>().CreateLogger(Category).LogError("real failure");

        Assert.That(records.Levels(Category), Is.EqualTo(new[] { LogLevel.Error }));
    }

    [Test]
    public void Pipeline_inside_a_probe_drops_warning_and_above_and_keeps_information()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger(Category);

        using (GrainStorageFencingProbeLogSuppression.Enter())
        {
            logger.LogInformation("kept");
            logger.LogWarning("dropped");
            logger.LogError("dropped");
            logger.LogCritical("dropped");
        }

        logger.LogError("after the probe");

        Assert.That(records.Levels(Category), Is.EqualTo(new[] { LogLevel.Information, LogLevel.Error }));
    }

    [Test]
    public void Pipeline_keeps_the_hosts_configured_levels_and_filters()
    {
        var (services, records) = BuildLogging(builder =>
        {
            builder.AddFilter("Verbose.Category", LogLevel.Debug);
            builder.AddFilter("Muted.Category", (Func<LogLevel, bool>)(_ => false));
        });
        using var _ = services;
        var factory = services.GetRequiredService<ILoggerFactory>();

        factory.CreateLogger("Verbose.Category").LogDebug("kept: configured at Debug");
        factory.CreateLogger("Other.Category").LogDebug("dropped: default minimum is Information");
        factory.CreateLogger("Muted.Category").LogError("dropped: the host's own filter");

        Assert.Multiple(() =>
        {
            Assert.That(records.Levels("Verbose.Category"), Is.EqualTo(new[] { LogLevel.Debug }));
            Assert.That(records.Levels("Other.Category"), Is.Empty);
            Assert.That(records.Levels("Muted.Category"), Is.Empty);
        });
    }

    [Test]
    public void PostConfigure_wraps_every_rule_and_preserves_its_selection_fields()
    {
        var options = new LoggerFilterOptions { MinLevel = LogLevel.Warning };
        options.Rules.Add(new LoggerFilterRule("Console", "Orleans", LogLevel.Debug, null));
        options.Rules.Add(new LoggerFilterRule(null, "Muted", null, (_, _, _) => false));
        var before = options.Rules.ToList();

        new GrainStorageFencingProbeLogFilter().PostConfigure(Options.DefaultName, options);

        Assert.That(options.Rules, Has.Count.EqualTo(3), "two wrapped rules and the added catch-all");
        for (var i = 0; i < before.Count; i++)
        {
            var rule = options.Rules[i];
            Assert.Multiple(() =>
            {
                Assert.That(rule.ProviderName, Is.EqualTo(before[i].ProviderName));
                Assert.That(rule.CategoryName, Is.EqualTo(before[i].CategoryName));
                Assert.That(rule.LogLevel, Is.EqualTo(before[i].LogLevel));
                Assert.That(rule.Filter, Is.Not.Null);
            });
        }

        Assert.That(options.Rules[0].Filter!("Console", "Orleans", LogLevel.Error), Is.True);
        Assert.That(options.Rules[1].Filter!(null, "Muted", LogLevel.Error), Is.False, "the original filter still applies");
    }

    [Test]
    public void PostConfigure_adds_a_catch_all_at_the_minimum_level_only_when_none_exists()
    {
        var bare = new LoggerFilterOptions { MinLevel = LogLevel.Warning };
        var covered = new LoggerFilterOptions();
        covered.Rules.Add(new LoggerFilterRule(null, null, LogLevel.Information, null));

        var filter = new GrainStorageFencingProbeLogFilter();
        filter.PostConfigure(Options.DefaultName, bare);
        filter.PostConfigure(Options.DefaultName, covered);

        var catchAll = bare.Rules.Single();
        Assert.Multiple(() =>
        {
            Assert.That(catchAll.ProviderName, Is.Null);
            Assert.That(catchAll.CategoryName, Is.Null);
            Assert.That(catchAll.LogLevel, Is.EqualTo(LogLevel.Warning));
            Assert.That(covered.Rules, Has.Count.EqualTo(1));
            Assert.That(covered.Rules[0].LogLevel, Is.EqualTo(LogLevel.Information));
        });
    }

    [Test]
    public void PostConfigure_twice_does_not_wrap_twice()
    {
        var options = new LoggerFilterOptions();
        options.Rules.Add(new LoggerFilterRule(null, "Orleans", LogLevel.Debug, null));
        var filter = new GrainStorageFencingProbeLogFilter();

        filter.PostConfigure(Options.DefaultName, options);
        var once = options.Rules.ToList();
        filter.PostConfigure(Options.DefaultName, options);

        Assert.That(options.Rules, Is.EqualTo(once));
    }

    [Test]
    public void PostConfigure_null_options_throws()
    {
        Assert.Throws<ArgumentNullException>(() => new GrainStorageFencingProbeLogFilter().PostConfigure(Options.DefaultName, null!));
    }

    [Test]
    public async Task Probe_against_a_provider_that_logs_rejected_writes_leaves_no_error()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var storage = new ErrorLoggingGrainStorage(
            new FencingGrainStorage(),
            services.GetRequiredService<ILogger<ErrorLoggingGrainStorage>>());

        var result = await GrainStorageFencingProbe.RunAsync(storage);

        Assert.That(result.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced));
        Assert.That(records.AtOrAbove(LogLevel.Warning), Is.Empty);
    }

    [Test]
    public async Task Provider_errors_after_the_probe_are_still_logged()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var storage = new ErrorLoggingGrainStorage(
            new FencingGrainStorage(),
            services.GetRequiredService<ILogger<ErrorLoggingGrainStorage>>());
        await GrainStorageFencingProbe.RunAsync(storage);

        var stale = new GrainState<GrainStorageFencingProbeState>(new GrainStorageFencingProbeState()) { ETag = "stale" };
        Assert.ThrowsAsync<InconsistentStateException>(() =>
            storage.WriteStateAsync("state", GrainStorageFencingProbe.ProbeGrainId, stale));

        Assert.That(records.AtOrAbove(LogLevel.Warning).Single().EventId, Is.EqualTo(ErrorLoggingGrainStorage.WriteError));
    }

    [Test]
    public async Task Provider_error_on_other_work_during_the_probe_is_still_logged()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger(Category);
        var probeWriting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var otherLogged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var other = Task.Run(async () =>
        {
            await probeWriting.Task;
            logger.LogError("another grain's write failed");
            otherLogged.SetResult();
        });
        var storage = new FencingGrainStorage
        {
            BeforeWrite = async ordinal =>
            {
                if (ordinal == 1)
                {
                    probeWriting.SetResult();
                    await otherLogged.Task;
                }
            },
        };

        await GrainStorageFencingProbe.RunAsync(storage);
        await other;

        Assert.That(records.Levels(Category), Is.EqualTo(new[] { LogLevel.Error }));
    }

    [Test]
    public async Task Check_reports_a_provider_fault_on_the_stale_write_itself()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var fault = new IOException("storage unreachable");
        var storage = new ErrorLoggingGrainStorage(
            new FencingGrainStorage { BeforeWrite = ordinal => ordinal == 3 ? throw fault : Task.CompletedTask },
            services.GetRequiredService<ILogger<ErrorLoggingGrainStorage>>());
        var check = CreateCheck(services, storage, LatticeGrainStorageFencingMode.Reject);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Inconclusive));
        var reported = records.AtOrAbove(LogLevel.Warning).Single();
        Assert.Multiple(() =>
        {
            Assert.That(reported.Category, Is.EqualTo(typeof(GrainStorageFencingCheck).FullName));
            Assert.That(reported.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(reported.Exception, Is.SameAs(fault), "the fault the provider logged reaches the log through the check");
        });
    }

    [Test]
    public void Check_still_reports_a_provider_that_accepts_the_stale_write_loudly()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var check = CreateCheck(services, new NonFencingGrainStorage(), LatticeGrainStorageFencingMode.Reject);

        Assert.ThrowsAsync<OrleansConfigurationException>(() => check.RunAsync(CancellationToken.None));

        var reported = records.AtOrAbove(LogLevel.Warning).Single();
        Assert.That(reported.Level, Is.EqualTo(LogLevel.Error));
        Assert.That(reported.Category, Is.EqualTo(typeof(GrainStorageFencingCheck).FullName));
    }

    [Test]
    public async Task Check_against_a_fenced_provider_logs_only_its_verdict()
    {
        var (services, records) = BuildLogging();
        using var _ = services;
        var storage = new ErrorLoggingGrainStorage(
            new FencingGrainStorage(),
            services.GetRequiredService<ILogger<ErrorLoggingGrainStorage>>());
        var check = CreateCheck(services, storage, LatticeGrainStorageFencingMode.Reject);

        await check.RunAsync(CancellationToken.None);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced));
        Assert.That(records.All.Select(r => (r.Category, r.Level)), Is.EqualTo(new[]
        {
            (typeof(GrainStorageFencingCheck).FullName, LogLevel.Information),
        }));
    }

    [Test]
    public void AddLattice_registers_the_filter_once()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });
        builder.AddLattice((_, _) => { });

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IPostConfigureOptions<LoggerFilterOptions>)
                && d.ImplementationType == typeof(GrainStorageFencingProbeLogFilter)),
            Is.EqualTo(1));
    }

    private static GrainStorageFencingCheck CreateCheck(
        IServiceProvider logging,
        IGrainStorage storage,
        LatticeGrainStorageFencingMode mode)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(LatticeOptions.StorageProviderName, storage);
        return new GrainStorageFencingCheck(
            services.BuildServiceProvider(),
            Options.Create(new LatticeGrainStorageFencingOptions { Mode = mode }),
            logging.GetRequiredService<ILogger<GrainStorageFencingCheck>>());
    }

    private static (ServiceProvider Services, CapturingLoggerProvider Records) BuildLogging(
        Action<ILoggingBuilder>? configure = null)
    {
        var records = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(builder =>
        {
            builder.AddProvider(records);
            configure?.Invoke(builder);
        });
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IPostConfigureOptions<LoggerFilterOptions>, GrainStorageFencingProbeLogFilter>());
        return (services.BuildServiceProvider(), records);
    }

    private sealed record Captured(string Category, LogLevel Level, EventId EventId, Exception? Exception);

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<Captured> _records = new();

        public IReadOnlyList<Captured> All => _records.ToArray();

        public IReadOnlyList<LogLevel> Levels(string category)
            => All.Where(r => r.Category == category).Select(r => r.Level).ToArray();

        public IReadOnlyList<Captured> AtOrAbove(LogLevel level)
            => All.Where(r => r.Level >= level).ToArray();

        public ILogger CreateLogger(string categoryName) => new Logger(categoryName, _records);

        public void Dispose()
        {
        }

        private sealed class Logger(string category, ConcurrentQueue<Captured> records) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
                => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
                => records.Enqueue(new Captured(category, logLevel, eventId, exception));
        }
    }
}
