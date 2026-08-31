using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for the per-view configuration entrypoint on
/// <see cref="LatticeViewsServiceCollectionExtensions"/>.
/// <para>
/// <c>ConfigureLatticeView</c> writes a <b>named</b> options instance, which is the
/// whole point: a maintainer resolves its settings with
/// <c>IOptionsMonitor&lt;LatticeViewOptions&gt;.Get(viewName)</c>, so a per-view
/// override must land under that name and must not disturb the global defaults other
/// views inherit. These tests pin that isolation and the argument guards.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeViewsServiceCollectionExtensionsTests
{
    private static ISiloBuilder BuilderOver(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    [Test]
    public void ConfigureLatticeView_applies_the_override_to_the_named_options_instance()
    {
        var services = new ServiceCollection();
        var builder = BuilderOver(services);

        builder.ConfigureLatticeView("by-owner", o => o.BatchSize = 17);

        var monitor = services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeViewOptions>>();
        Assert.That(monitor.Get("by-owner").BatchSize, Is.EqualTo(17));
    }

    [Test]
    public void ConfigureLatticeView_leaves_other_views_on_the_defaults()
    {
        var services = new ServiceCollection();
        var builder = BuilderOver(services);
        var defaultBatchSize = new LatticeViewOptions().BatchSize;

        builder.ConfigureLatticeView("by-owner", o => o.BatchSize = 17);

        var monitor = services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeViewOptions>>();
        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get("by-status").BatchSize, Is.EqualTo(defaultBatchSize));
            Assert.That(monitor.CurrentValue.BatchSize, Is.EqualTo(defaultBatchSize));
        });
    }

    [Test]
    public void ConfigureLatticeView_composes_repeated_configuration_of_the_same_view()
    {
        var services = new ServiceCollection();
        var builder = BuilderOver(services);

        builder.ConfigureLatticeView("by-owner", o => o.BatchSize = 17);
        builder.ConfigureLatticeView("by-owner", o => o.AggregationFanout = 5);

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeViewOptions>>()
            .Get("by-owner");
        Assert.Multiple(() =>
        {
            Assert.That(options.BatchSize, Is.EqualTo(17));
            Assert.That(options.AggregationFanout, Is.EqualTo(5));
        });
    }

    [Test]
    public void ConfigureLatticeView_returns_the_same_builder_for_chaining()
    {
        var builder = BuilderOver(new ServiceCollection());

        var returned = builder.ConfigureLatticeView("by-owner", _ => { });

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void ConfigureLatticeView_rejects_a_null_builder()
    {
        Assert.Throws<ArgumentNullException>(
            () => ((ISiloBuilder)null!).ConfigureLatticeView("by-owner", _ => { }));
    }

    [Test]
    public void ConfigureLatticeView_rejects_a_blank_view_name()
    {
        var builder = BuilderOver(new ServiceCollection());

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => builder.ConfigureLatticeView(null!, _ => { }));
            Assert.Throws<ArgumentException>(() => builder.ConfigureLatticeView(string.Empty, _ => { }));
        });
    }

    [Test]
    public void ConfigureLatticeView_rejects_a_null_configure_delegate()
    {
        var builder = BuilderOver(new ServiceCollection());

        Assert.Throws<ArgumentNullException>(() => builder.ConfigureLatticeView("by-owner", null!));
    }
}
