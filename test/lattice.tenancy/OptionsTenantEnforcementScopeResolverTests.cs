using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="OptionsTenantEnforcementScopeResolver"/>: the default
/// resolver that maps every tenant to the cluster-wide default enforcement scope
/// and reflects a live option change.
/// </summary>
[TestFixture]
public sealed class OptionsTenantEnforcementScopeResolverTests
{
    private static IOptionsMonitor<TenantUsageAccountingOptions> Monitor(TenantEnforcementScope scope)
    {
        var monitor = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        monitor.CurrentValue.Returns(new TenantUsageAccountingOptions { DefaultEnforcementScope = scope });
        return monitor;
    }

    [Test]
    public void Constructor_null_options_throws()
    {
        Assert.That(() => new OptionsTenantEnforcementScopeResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_returns_the_configured_default_scope()
    {
        var resolver = new OptionsTenantEnforcementScopeResolver(Monitor(TenantEnforcementScope.PerCluster));

        Assert.That(resolver.Resolve(TenantId.Parse("acme")), Is.EqualTo(TenantEnforcementScope.PerCluster));
    }

    [Test]
    public void Resolve_reflects_a_live_option_change()
    {
        var monitor = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        monitor.CurrentValue.Returns(
            new TenantUsageAccountingOptions { DefaultEnforcementScope = TenantEnforcementScope.GlobalConverged },
            new TenantUsageAccountingOptions { DefaultEnforcementScope = TenantEnforcementScope.PerCluster });
        var resolver = new OptionsTenantEnforcementScopeResolver(monitor);

        Assert.Multiple(() =>
        {
            Assert.That(resolver.Resolve(TenantId.Parse("acme")), Is.EqualTo(TenantEnforcementScope.GlobalConverged));
            Assert.That(resolver.Resolve(TenantId.Parse("acme")), Is.EqualTo(TenantEnforcementScope.PerCluster));
        });
    }
}
