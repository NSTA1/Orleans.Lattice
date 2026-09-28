using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.Shell;

/// <summary>
/// The Shell's registration root: it registers the design services, is safe to
/// call twice, and declares - and calls - exactly one partial registration
/// method per epic #3807 owner, so every later item has its seam without editing
/// the root file.
/// </summary>
[TestFixture]
public sealed class ShellServiceCollectionExtensionsTests
{
    private const string RootFile = "src/lattice.explorer/Shell/ShellServiceCollectionExtensions.cs";

    /// <summary>The owners, in the order the root calls them.</summary>
    private static readonly string[] Owners =
    [
        "Transport",
        "Session",
        "Chrome",
        "AppFrame",
        "AppsCatalogue",
        "App",
        "Data",
        "Access",
        "Tenancy",
        "Schema",
        "Replication",
        "Backups",
        "Telemetry",
        "Cluster",
    ];

    [Test]
    public void It_rejects_a_null_collection()
    {
        Assert.That(() => ((IServiceCollection)null!).AddLatticeExplorerShell(), Throws.ArgumentNullException);
    }

    [Test]
    public void It_registers_the_toast_queue_once_per_circuit()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();
        var toast = services.Single(descriptor => descriptor.ServiceType == typeof(LtToastService));

        Assert.That(toast.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void A_second_call_registers_nothing_more()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();
        var count = services.Count;

        services.AddLatticeExplorerShell();

        Assert.That(services, Has.Count.EqualTo(count));
    }

    [Test]
    public void It_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();

        Assert.That(services.AddLatticeExplorerShell(), Is.SameAs(services));
    }

    [Test]
    [TestCaseSource(nameof(Owners))]
    public void Each_owner_has_one_partial_registration_seam(string owner)
    {
        var source = File.ReadAllText(ShellStylesheets.Absolute(RootFile));

        Assert.Multiple(() =>
        {
            Assert.That(
                Regex.Matches(source, $@"static partial void Add{owner}\(IServiceCollection services\);").Count,
                Is.EqualTo(1),
                $"the root must declare exactly one 'static partial void Add{owner}(IServiceCollection services);'");
            Assert.That(
                Regex.Matches(source, $@"^\s*Add{owner}\(services\);", RegexOptions.Multiline).Count,
                Is.EqualTo(1),
                $"the root must call Add{owner}(services) exactly once");
        });
    }

    [Test]
    public void The_root_calls_the_design_system_first_and_the_owners_in_dependency_order()
    {
        var source = File.ReadAllText(ShellStylesheets.Absolute(RootFile));
        var calls = Regex.Matches(source, @"^\s*Add(?<name>[A-Za-z]+)\(services\);", RegexOptions.Multiline)
            .Select(match => match.Groups["name"].Value)
            .ToArray();

        Assert.That(calls, Is.EqualTo(new[] { "Design" }.Concat(Owners)));
    }

    [Test]
    public void The_root_declares_no_other_partial_seam()
    {
        var source = File.ReadAllText(ShellStylesheets.Absolute(RootFile));
        var declared = Regex.Matches(source, @"static partial void Add(?<name>[A-Za-z]+)\(")
            .Select(match => match.Groups["name"].Value);

        Assert.That(declared, Is.EquivalentTo(Owners));
    }
}
