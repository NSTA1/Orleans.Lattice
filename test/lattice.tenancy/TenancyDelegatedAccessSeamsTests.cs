using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the delegated tenant access administration flag and the two
/// active seams tenancy installs in place of V1's null defaults (epic #4154, T1):
/// <see cref="DelegatedTenantAccessFlag"/>, <see cref="TenancyTenantRuleLayer"/>,
/// membership's tenant group claim filter wired to the flag, their registration
/// by <c>AddLatticeTenancy</c>, and the start-up
/// <see cref="TenancyPostureLogger"/>.
/// Options changes are driven through a hand-rolled monitor, so every
/// notification is synchronous and exact.
/// </summary>
[TestFixture]
public sealed class TenancyDelegatedAccessSeamsTests
{
    // ---- DelegatedTenantAccessFlag --------------------------------------

    [Test]
    public void Flag_defaults_off_and_reads_the_configured_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LatticeTenancyOptions().DelegatedAccessAdministrationEnabled, Is.False, "opt-in, off by default");
            Assert.That(new DelegatedTenantAccessFlag(new Monitor(enabled: false)).IsEnabled, Is.False);
            Assert.That(new DelegatedTenantAccessFlag(new Monitor(enabled: true)).IsEnabled, Is.True);
            Assert.That(DelegatedTenantAccessFlag.Disabled.IsEnabled, Is.False);
        });
    }

    [Test]
    public void Flag_raises_Changed_only_when_the_value_moves()
    {
        var monitor = new Monitor(enabled: false);
        using var flag = new DelegatedTenantAccessFlag(monitor);
        var raised = 0;
        flag.Changed += () => raised++;

        monitor.Publish(enabled: false);
        Assert.That(raised, Is.Zero, "an unrelated reload is not a change");

        monitor.Publish(enabled: true);
        Assert.That((flag.IsEnabled, raised), Is.EqualTo((true, 1)));

        monitor.Publish(enabled: true);
        monitor.Publish(enabled: false);
        Assert.That((flag.IsEnabled, raised), Is.EqualTo((false, 2)));
    }

    [Test]
    public void Flag_ignores_a_named_options_instance()
    {
        var monitor = new Monitor(enabled: false);
        using var flag = new DelegatedTenantAccessFlag(monitor);

        monitor.Publish(enabled: true, name: "other");

        Assert.That(flag.IsEnabled, Is.False);
    }

    [Test]
    public void Flag_dispose_stops_following_the_monitor()
    {
        var monitor = new Monitor(enabled: false);
        var flag = new DelegatedTenantAccessFlag(monitor);

        flag.Dispose();
        monitor.Publish(enabled: true);

        Assert.That(flag.IsEnabled, Is.False);
        Assert.That(monitor.Listeners, Is.Zero);
    }

    [Test]
    public void Flag_null_monitor_throws()
    {
        Assert.That(() => new DelegatedTenantAccessFlag(null!), Throws.ArgumentNullException);
    }

    // ---- TenancyTenantRuleLayer ----------------------------------------

    [Test]
    public void RuleLayer_IsActive_follows_the_flag()
    {
        var flag = new DelegatedTenantAccessFlag(false);
        ITenantRuleLayer layer = new TenancyTenantRuleLayer(flag);
        Assert.That(layer.IsActive, Is.False);

        flag.Set(true);

        Assert.That(layer.IsActive, Is.True);
    }

    [Test]
    public void RuleLayer_null_flag_throws()
    {
        Assert.That(() => new TenancyTenantRuleLayer(null!), Throws.ArgumentNullException);
    }

    // ---- Registration ---------------------------------------------------

    [Test]
    public void AddLatticeTenancy_replaces_the_null_rule_layer_with_a_flag_driven_one()
    {
        var builder = NewBuilderWithDependencies();
        builder.Services.AddSingleton(Substitute.For<ITenantRuleLayer>());

        builder.AddLatticeTenancy(o => o.DelegatedAccessAdministrationEnabled = true);

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Single(d => d.ServiceType == typeof(ITenantRuleLayer)).ImplementationType,
                Is.EqualTo(typeof(TenancyTenantRuleLayer)));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(DelegatedTenantAccessFlag)), Is.EqualTo(1));
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)
                    && d.ImplementationType == typeof(TenancyPostureLogger)),
                Is.EqualTo(1));
        });

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantRuleLayer>().IsActive, Is.True);
    }

    [Test]
    public void AddLatticeTenancy_without_the_flag_registers_an_inactive_rule_layer()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantRuleLayer>().IsActive, Is.False);
    }

    [Test]
    public void AddLatticeTenancy_replaces_the_null_claim_filter_with_one_that_follows_the_flag()
    {
        var builder = NewBuilderWithDependencies();
        builder.Services.AddSingleton(Substitute.For<ITenantGroupClaimFilter>());

        builder.AddLatticeTenancy();

        Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantGroupClaimFilter)), Is.EqualTo(1));

        using var provider = builder.Services.BuildServiceProvider();
        var filter = provider.GetRequiredService<ITenantGroupClaimFilter>();
        var flag = provider.GetRequiredService<DelegatedTenantAccessFlag>();
        Assert.Multiple(() =>
        {
            Assert.That(filter.GetType().Name, Is.EqualTo("TenantGroupClaimFilter"), "membership's active filter, not a tenancy copy");
            Assert.That(filter.IsActive, Is.False, "off by default");
        });

        flag.Set(true);

        Assert.That(filter.IsActive, Is.True, "the filter reads the live flag");
    }

    [Test]
    public void AddLatticeTenancy_claim_filter_strips_the_reserved_tenant_namespace()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy(o => o.DelegatedAccessAdministrationEnabled = true);
        using var provider = builder.Services.BuildServiceProvider();
        var groups = new HashSet<string>(StringComparer.Ordinal) { "t/acme/editors", "t/default/x", "entra-sales" };

        provider.GetRequiredService<ITenantGroupClaimFilter>().Filter(groups);

        Assert.That(groups, Is.EquivalentTo(new[] { "entra-sales" }));
    }

    [Test]
    public void AddLatticeTenancy_claim_filter_IsActive_read_allocates_nothing()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();
        using var provider = builder.Services.BuildServiceProvider();
        var filter = provider.GetRequiredService<ITenantGroupClaimFilter>();

        var growth = Orleans.Lattice.Tests.Fakes.AllocationProbe.Growth(
            prepare: _ => filter,
            measure: static (state, size) =>
            {
                long active = 0;
                for (var i = 0; i < size; i++)
                {
                    active += state.IsActive ? 1 : 0;
                }

                Orleans.Lattice.Tests.Fakes.AllocationProbe.ScalarSink += active;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }

    [Test]
    public void Flag_ReadIsEnabled_matches_IsEnabled()
    {
        var flag = new DelegatedTenantAccessFlag(false);
        Func<bool> read = flag.ReadIsEnabled;
        Assert.That(read(), Is.False);

        flag.Set(true);

        Assert.That(read(), Is.EqualTo(flag.IsEnabled).And.True);
    }

    // ---- Posture logger -------------------------------------------------

    [TestCase(false)]
    [TestCase(true)]
    public async Task PostureLogger_logs_the_flag_once_at_start(bool enabled)
    {
        var logger = new CapturingLogger();
        var posture = new TenancyPostureLogger(logger, new Monitor(enabled));

        await posture.StartAsync(CancellationToken.None);
        await posture.StopAsync(CancellationToken.None);

        Assert.That(logger.Messages, Has.Count.EqualTo(1));
        Assert.That(logger.Messages[0], Does.Contain($"DelegatedAccessAdministrationEnabled={enabled}"));
    }

    private static CovSiloBuilder NewBuilderWithDependencies()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        return builder;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class CovSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }

    /// <summary>An options monitor whose change notifications the test publishes synchronously.</summary>
    private sealed class Monitor(bool enabled) : IOptionsMonitor<LatticeTenancyOptions>
    {
        private readonly List<Action<LatticeTenancyOptions, string?>> _listeners = [];

        public LatticeTenancyOptions CurrentValue { get; private set; } = new() { DelegatedAccessAdministrationEnabled = enabled };

        public int Listeners => _listeners.Count;

        public LatticeTenancyOptions Get(string? name) => CurrentValue;

        public IDisposable OnChange(Action<LatticeTenancyOptions, string?> listener)
        {
            _listeners.Add(listener);
            return new Subscription(() => _listeners.Remove(listener));
        }

        public void Publish(bool enabled, string? name = null)
        {
            var value = new LatticeTenancyOptions { DelegatedAccessAdministrationEnabled = enabled };
            if (name is null)
            {
                CurrentValue = value;
            }

            foreach (var listener in _listeners.ToArray())
            {
                listener(value, name ?? Options.DefaultName);
            }
        }

        private sealed class Subscription(Action dispose) : IDisposable
        {
            public void Dispose() => dispose();
        }
    }

    /// <summary>Captures every formatted log message.</summary>
    private sealed class CapturingLogger : ILogger<TenancyPostureLogger>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }
}
