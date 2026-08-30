using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginAccessGatesTests
{
    private IExplorerPluginHostContext Context { get; set; } = null!;

    [SetUp]
    public void SetUp() => Context = PluginTestHost.Create(new FakeExplorerPlugin("a")).Contexts.Create("a");

    [Test]
    public async Task Fixed_gates_report_their_state()
    {
        var allowed = await ExplorerPluginAccessGates.Allowed.ProbeAsync(Context);
        var denied = await ExplorerPluginAccessGates.Denied.ProbeAsync(Context);
        var auth = await ExplorerPluginAccessGates.AuthenticationRequired.ProbeAsync(Context);
        var unavailable = await ExplorerPluginAccessGates.Unavailable.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(allowed, Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(denied, Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(auth, Is.EqualTo(ExplorerPluginAccess.AuthenticationRequired));
            Assert.That(unavailable, Is.EqualTo(ExplorerPluginAccess.Unavailable));
        });
    }

    [Test]
    public void Fixed_gates_are_cached_singletons()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccessGates.Allowed, Is.SameAs(ExplorerPluginAccessGates.Allowed));
            Assert.That(
                ExplorerPluginAccessGates.Fixed(ExplorerPluginAccess.Allowed),
                Is.SameAs(ExplorerPluginAccessGates.Allowed));
            Assert.That(
                ExplorerPluginAccessGates.Fixed(ExplorerPluginAccess.Denied),
                Is.SameAs(ExplorerPluginAccessGates.Denied));
            Assert.That(
                ExplorerPluginAccessGates.Fixed(ExplorerPluginAccess.AuthenticationRequired),
                Is.SameAs(ExplorerPluginAccessGates.AuthenticationRequired));
            Assert.That(
                ExplorerPluginAccessGates.Fixed(ExplorerPluginAccess.Unavailable),
                Is.SameAs(ExplorerPluginAccessGates.Unavailable));
        });
    }

    [Test]
    public async Task Fixed_with_a_reason_reports_that_exact_result()
    {
        var access = ExplorerPluginAccess.Deny("no admin role");

        var gate = ExplorerPluginAccessGates.Fixed(access);

        Assert.That(await gate.ProbeAsync(Context), Is.EqualTo(access));
    }

    [Test]
    public async Task Delegate_gate_runs_the_supplied_probe_with_the_context()
    {
        IExplorerPluginHostContext? observed = null;
        var gate = ExplorerPluginAccessGates.FromDelegate((context, _) =>
        {
            observed = context;
            return ValueTask.FromResult(ExplorerPluginAccess.Allowed);
        });

        var access = await gate.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access, Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(observed, Is.SameAs(Context));
        });
    }

    [Test]
    public async Task Delegate_gate_receives_the_cancellation_token()
    {
        using var cts = new CancellationTokenSource();
        var observed = CancellationToken.None;

        var gate = ExplorerPluginAccessGates.FromDelegate((_, token) =>
        {
            observed = token;
            return ValueTask.FromResult(ExplorerPluginAccess.Allowed);
        });

        await gate.ProbeAsync(Context, cts.Token);

        Assert.That(observed, Is.EqualTo(cts.Token));
    }

    [Test]
    public void FromDelegate_rejects_a_null_probe()
    {
        Assert.That(() => ExplorerPluginAccessGates.FromDelegate(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Gates_reject_a_null_context()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ExplorerPluginAccessGates.Allowed.ProbeAsync(null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await ExplorerPluginAccessGates
                    .FromDelegate(static (_, _) => ValueTask.FromResult(ExplorerPluginAccess.Allowed))
                    .ProbeAsync(null!),
                Throws.ArgumentNullException);
        });
    }
}
