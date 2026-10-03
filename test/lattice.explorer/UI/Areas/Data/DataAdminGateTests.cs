using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Data area's fail-closed administration gate: a head that serves no
/// tree-administration facade, one whose resolution throws, a probe that faults
/// and a probe that answers nothing all read as "no", so an action the caller
/// cannot perform is never drawn.
/// </summary>
/// <remarks>
/// Every refusal below returns the same <see langword="false"/> a genuine
/// ungranted caller returns, so no page-level test can distinguish them - a
/// reconcile button is equally absent whether the facade is missing, broken, or
/// simply not granted. Only a fixture against the gate, with a service provider
/// that can be made to fail each way, tells them apart.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataAdminGateTests
{
    private const string Tree = "orders";

    [Test]
    public async Task A_granted_caller_may_administer_the_tree()
    {
        using var services = Circuit(Granting(canAdminister: true));

        Assert.That(await new DataAdminGate(services).CanAdministerAsync(Tree), Is.True);
    }

    [Test]
    public async Task An_ungranted_caller_may_not()
    {
        using var services = Circuit(Granting(canAdminister: false));

        Assert.That(await new DataAdminGate(services).CanAdministerAsync(Tree), Is.False);
    }

    [Test]
    public async Task A_head_that_serves_no_tree_administration_facade_grants_nothing()
    {
        using var services = Circuit();
        var gate = new DataAdminGate(services);

        Assert.Multiple(async () =>
        {
            Assert.That(gate.Admin, Is.Null, "an absent keyed registration resolves to null rather than throwing");
            Assert.That(await gate.CanAdministerAsync(Tree), Is.False);
        });
    }

    [Test]
    public async Task A_facade_whose_resolution_throws_reads_as_a_head_that_serves_none()
    {
        // Distinct from the absent registration above: that one returns null from
        // GetKeyedService, this one faults inside the factory, and they are two
        // different arms of the gate.
        using var services = Circuit(collection => collection.AddKeyedSingleton<ILatticeTreeAdmin>(
            ShellFacades.Key,
            (_, _) => throw new InvalidOperationException("the facade could not be built")));
        var gate = new DataAdminGate(services);

        Assert.Multiple(async () =>
        {
            Assert.That(gate.Admin, Is.Null);
            Assert.That(await gate.CanAdministerAsync(Tree), Is.False);
        });
    }

    [Test]
    public async Task A_capability_probe_that_faults_grants_nothing()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("the cluster is unreachable"));
        using var services = Circuit(Serving(admin));

        Assert.That(await new DataAdminGate(services).CanAdministerAsync(Tree), Is.False, "a probe that cannot answer grants nothing");
    }

    [Test]
    public async Task A_capability_probe_that_refuses_the_caller_grants_nothing()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Throws(new UnauthorizedAccessException("no grant"));
        using var services = Circuit(Serving(admin));

        Assert.That(await new DataAdminGate(services).CanAdministerAsync(Tree), Is.False);
    }

    [Test]
    public async Task A_capability_probe_that_answers_nothing_at_all_grants_nothing()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeTreeAdminCapabilities>(null!));
        using var services = Circuit(Serving(admin));

        Assert.That(await new DataAdminGate(services).CanAdministerAsync(Tree), Is.False, "no capabilities is not a grant");
    }

    [Test]
    public async Task The_verdict_is_probed_once_per_tree_and_reused_for_the_same_caller()
    {
        var admin = Granting(canAdminister: true, out var probed);
        using var services = Circuit(Serving(admin));
        var gate = new DataAdminGate(services);

        var first = await gate.CanAdministerAsync(Tree);
        var second = await gate.CanAdministerAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.True);
            Assert.That(probed(), Is.EqualTo(1), "the facade is asked once per tree per caller");
        });
    }

    [Test]
    public async Task Each_tree_is_probed_on_its_own()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Capabilities(call.Arg<string>(), call.Arg<string>() == Tree)));
        using var services = Circuit(Serving(admin));
        var gate = new DataAdminGate(services);

        Assert.Multiple(async () =>
        {
            Assert.That(await gate.CanAdministerAsync(Tree), Is.True);
            Assert.That(await gate.CanAdministerAsync("billing"), Is.False, "a grant on one tree is not a grant on another");
        });
    }

    [Test]
    public async Task A_caller_who_stops_waiting_does_not_discard_the_answer_for_the_next_read()
    {
        var release = new TaskCompletionSource<LatticeTreeAdminCapabilities>();
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(release.Task);
        using var services = Circuit(Serving(admin));
        var gate = new DataAdminGate(services);
        using var abandoned = new CancellationTokenSource();

        var waiting = gate.CanAdministerAsync(Tree, abandoned.Token);
        await abandoned.CancelAsync();
        release.SetResult(Capabilities(Tree, admin: true));

        Assert.Multiple(async () =>
        {
            Assert.That(async () => await waiting, Throws.InstanceOf<OperationCanceledException>(), "the abandoned wait is cancelled");
            Assert.That(await gate.CanAdministerAsync(Tree), Is.True, "the probe it started still answers the next read");
        });
    }

    [Test]
    public void The_gate_refuses_to_be_built_without_the_circuits_services()
    {
        Assert.That(() => new DataAdminGate(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void A_tree_with_no_id_is_refused_before_anything_is_probed()
    {
        using var services = Circuit(Granting(canAdminister: true));
        var gate = new DataAdminGate(services);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await gate.CanAdministerAsync(null!), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await gate.CanAdministerAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void The_gate_surfaces_the_heads_own_facade_when_one_is_served()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        using var services = Circuit(Serving(admin));

        Assert.That(new DataAdminGate(services).Admin, Is.SameAs(admin));
    }

    private static Action<IServiceCollection> Serving(ILatticeTreeAdmin admin) =>
        collection => collection.AddKeyedSingleton(ShellFacades.Key, admin);

    private static Action<IServiceCollection> Granting(bool canAdminister) =>
        Serving(Granting(canAdminister, out _));

    private static ILatticeTreeAdmin Granting(bool canAdminister, out Func<int> probed)
    {
        var count = 0;
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                Interlocked.Increment(ref count);
                return Task.FromResult(Capabilities(call.Arg<string>(), canAdminister));
            });
        probed = () => Volatile.Read(ref count);
        return admin;
    }

    private static LatticeTreeAdminCapabilities Capabilities(string treeId, bool admin) => new()
    {
        TreeId = treeId,
        CanAdministerTree = admin,
        Schema = new LatticeSchemaCapabilities { TreeId = treeId },
    };

    private static ServiceProvider Circuit(Action<IServiceCollection>? configure = null)
    {
        var collection = new ServiceCollection();
        configure?.Invoke(collection);
        return collection.BuildServiceProvider();
    }
}
