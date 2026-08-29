using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.DesignSystem;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the design system's service registration.
/// </summary>
[TestFixture]
public sealed class LatticeDesignSystemServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeExplorerDesignSystem_rejects_a_null_collection()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeExplorerDesignSystem(),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void AddLatticeExplorerDesignSystem_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();

        Assert.That(services.AddLatticeExplorerDesignSystem(), Is.SameAs(services));
    }

    [Test]
    public void AddLatticeExplorerDesignSystem_registers_the_viewport_seam()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerDesignSystem();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetService<ILatticeViewport>(), Is.InstanceOf<LatticeViewport>());
    }

    [Test]
    public void The_viewport_is_scoped_so_two_circuits_never_share_a_breakpoint()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerDesignSystem();

        using var provider = services.BuildServiceProvider();
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        var firstViewport = first.ServiceProvider.GetRequiredService<ILatticeViewport>();
        var secondViewport = second.ServiceProvider.GetRequiredService<ILatticeViewport>();

        Assert.Multiple(() =>
        {
            Assert.That(firstViewport, Is.Not.SameAs(secondViewport));
            Assert.That(
                first.ServiceProvider.GetRequiredService<ILatticeViewport>(),
                Is.SameAs(firstViewport),
                "the same scope must reuse one viewport");
        });
    }

    [Test]
    public void AddLatticeExplorerDesignSystem_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddLatticeExplorerDesignSystem();
        services.AddLatticeExplorerDesignSystem();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(ILatticeViewport)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeExplorerDesignSystem_does_not_replace_a_caller_supplied_viewport()
    {
        var services = new ServiceCollection();
        services.AddScoped<ILatticeViewport, PinnedViewport>();

        services.AddLatticeExplorerDesignSystem();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<ILatticeViewport>(), Is.InstanceOf<PinnedViewport>());
    }

    /// <summary>A stand-in viewport a head could substitute for the default.</summary>
    private sealed class PinnedViewport : ILatticeViewport
    {
        public Orleans.Lattice.Explorer.DesignSystem.Tokens.LatticeBreakpoint Breakpoint =>
            Orleans.Lattice.Explorer.DesignSystem.Tokens.LatticeBreakpoint.Compact;

        public bool IsMeasured => true;

        public event Action<Orleans.Lattice.Explorer.DesignSystem.Tokens.LatticeBreakpoint>? BreakpointChanged
        {
            add { }
            remove { }
        }

        public bool SetBreakpoint(Orleans.Lattice.Explorer.DesignSystem.Tokens.LatticeBreakpoint breakpoint) => false;

        public bool SetViewportWidth(int viewportWidth) => false;
    }
}
