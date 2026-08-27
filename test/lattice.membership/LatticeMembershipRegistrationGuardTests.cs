using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the composition-root contract of
/// <see cref="LatticeMembershipServiceCollectionExtensions"/>: the ordering guard
/// that fails fast when <c>AddLattice</c> has not run, the argument guards, the
/// re-entrancy rule (a repeat call layers a further configure delegate but does
/// the structural wiring only once, because the mutation-observer registration is
/// not idempotent), and the standalone
/// <see cref="LatticeMembershipServiceCollectionExtensions.ConfigureLatticeMembership"/>
/// overload.
/// </summary>
[TestFixture]
public sealed class LatticeMembershipRegistrationGuardTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CoreRegistered()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    private static ISiloBuilder CoreMissing()
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(new ServiceCollection());
        return builder;
    }

    [Test]
    public void AddLatticeMembership_before_AddLattice_fails_fast_with_an_actionable_message()
    {
        var builder = CoreMissing();

        Assert.That(
            () => builder.AddLatticeMembership(),
            Throws.InvalidOperationException.With.Message.Contains("must be called after AddLattice()"));
    }

    [Test]
    public void AddLatticeMembership_rejects_a_null_builder()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeMembership(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeMembership_returns_the_same_builder_for_chaining()
    {
        var (builder, _) = CoreRegistered();

        Assert.That(builder.AddLatticeMembership(), Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeMembership_applies_a_supplied_configure_delegate()
    {
        var (builder, services) = CoreRegistered();

        builder.AddLatticeMembership(o => o.ResolutionCacheTtl = TimeSpan.FromSeconds(30));

        using var provider = services.BuildServiceProvider();
        Assert.That(
            provider.GetRequiredService<IOptions<LatticeMembershipOptions>>().Value.ResolutionCacheTtl,
            Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void A_repeat_call_layers_the_second_configure_delegate_over_the_first()
    {
        var (builder, services) = CoreRegistered();

        builder.AddLatticeMembership(o =>
        {
            o.ResolutionCacheTtl = TimeSpan.FromSeconds(30);
            o.GroupMergeMode = SubjectGroupMergeMode.TokenOnly;
        });
        builder.AddLatticeMembership(o => o.ResolutionCacheTtl = TimeSpan.FromSeconds(90));

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeMembershipOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.ResolutionCacheTtl, Is.EqualTo(TimeSpan.FromSeconds(90)),
                "the later delegate wins for the property it sets");
            Assert.That(options.GroupMergeMode, Is.EqualTo(SubjectGroupMergeMode.TokenOnly),
                "the earlier delegate's other settings survive");
        });
    }

    [Test]
    public void A_repeat_call_does_the_structural_wiring_only_once()
    {
        var (builder, services) = CoreRegistered();

        builder.AddLatticeMembership();
        var afterFirst = services.Count(d => d.ServiceType == typeof(IMutationObserver));

        builder.AddLatticeMembership();
        var afterSecond = services.Count(d => d.ServiceType == typeof(IMutationObserver));

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.GreaterThan(0),
                "the first call must register the change-feed observer");
            Assert.That(afterSecond, Is.EqualTo(afterFirst),
                "the observer registration is not idempotent under TryAdd, so a repeat call must skip it");
        });
    }

    [Test]
    public void A_repeat_call_registers_the_marker_only_once()
    {
        var (builder, services) = CoreRegistered();

        builder.AddLatticeMembership();
        builder.AddLatticeMembership();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(MembershipRegistrationMarker)),
            Is.EqualTo(1));
    }

    [Test]
    public void ConfigureLatticeMembership_layers_an_additional_delegate()
    {
        var (builder, services) = CoreRegistered();
        builder.AddLatticeMembership();

        var returned = builder.ConfigureLatticeMembership(o => o.EnableDurableHistoryView = false);

        using var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(builder));
            Assert.That(
                provider.GetRequiredService<IOptions<LatticeMembershipOptions>>().Value.EnableDurableHistoryView,
                Is.False);
        });
    }

    [Test]
    public void ConfigureLatticeMembership_rejects_null_arguments()
    {
        var (builder, _) = CoreRegistered();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => ((ISiloBuilder)null!).ConfigureLatticeMembership(_ => { }),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.ConfigureLatticeMembership(null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddLatticeMembership_registers_the_membership_options_validator()
    {
        var (builder, services) = CoreRegistered();

        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();
        Assert.That(
            provider.GetServices<IValidateOptions<LatticeMembershipOptions>>()
                .Any(v => v is LatticeMembershipOptionsValidator),
            Is.True);
    }
}
