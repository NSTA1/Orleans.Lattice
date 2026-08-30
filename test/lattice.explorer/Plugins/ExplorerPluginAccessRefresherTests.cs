using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginAccessRefresherTests
{
    [Test]
    public async Task RefreshAsync_files_each_plugins_own_decision()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("a", gate: ExplorerPluginAccessGates.Allowed),
            new FakeExplorerPlugin("b", gate: ExplorerPluginAccessGates.Unavailable),
            new FakeExplorerPlugin("c", gate: ExplorerPluginAccessGates.AuthenticationRequired));

        await host.Refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(host.Store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(host.Store.Get("b"), Is.EqualTo(ExplorerPluginAccess.Unavailable));
            Assert.That(host.Store.Get("c"), Is.EqualTo(ExplorerPluginAccess.AuthenticationRequired));
        });
    }

    [Test]
    public async Task A_throwing_gate_denies_its_own_plugin_and_does_not_disturb_a_sibling()
    {
        var faulting = ControllableExplorerPluginAccessGate.Throwing(
            new InvalidOperationException("probe exploded"));

        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("faulty", gate: faulting),
            new FakeExplorerPlugin("healthy", gate: ExplorerPluginAccessGates.Allowed));

        await host.Refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(host.Store.Get("faulty").State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(host.Store.Get("faulty").Reason, Is.EqualTo("probe exploded"));
            Assert.That(host.Store.Get("healthy"), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public void A_throwing_gate_does_not_propagate_out_of_RefreshAsync()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("faulty", gate: ControllableExplorerPluginAccessGate.Throwing(new Exception("x"))));

        Assert.That(async () => await host.Refresher.RefreshAsync(), Throws.Nothing);
    }

    [Test]
    public async Task A_gate_faulting_asynchronously_still_denies_only_its_own_plugin()
    {
        var faulting = ControllableExplorerPluginAccessGate.Hanging();
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("faulty", gate: faulting),
            new FakeExplorerPlugin("healthy", gate: ExplorerPluginAccessGates.Allowed));

        var refresh = host.Refresher.RefreshAsync();
        faulting.Fault(new InvalidOperationException("async boom"));
        await refresh;

        Assert.Multiple(() =>
        {
            Assert.That(host.Store.Get("faulty").State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(host.Store.Get("faulty").Reason, Is.EqualTo("async boom"));
            Assert.That(host.Store.Get("healthy"), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public async Task An_outstanding_probe_does_not_hold_back_a_siblings_decision()
    {
        var hanging = ControllableExplorerPluginAccessGate.Hanging();
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("hanging", gate: hanging),
            new FakeExplorerPlugin("healthy", gate: ExplorerPluginAccessGates.Allowed));

        // RefreshAsync starts every probe before it awaits any of them, and a
        // synchronously-answering gate has already been filed by the time the
        // returned task is handed back. No clock is involved: the assertion runs
        // while the hanging probe is still outstanding by construction.
        var refresh = host.Refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(refresh.IsCompleted, Is.False, "the hanging probe should still be outstanding");
            Assert.That(host.Store.Get("healthy"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(host.Store.Get("hanging"), Is.EqualTo(ExplorerPluginAccess.Denied));
        });

        hanging.Complete(ExplorerPluginAccess.Allowed);
        await refresh;

        Assert.That(host.Store.Get("hanging"), Is.EqualTo(ExplorerPluginAccess.Allowed));
    }

    [Test]
    public async Task A_cancelled_probe_leaves_its_plugin_denied()
    {
        var hanging = ControllableExplorerPluginAccessGate.Hanging();
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("hanging", gate: hanging),
            new FakeExplorerPlugin("healthy", gate: ExplorerPluginAccessGates.Allowed));

        var refresh = host.Refresher.RefreshAsync();
        hanging.Fault(new OperationCanceledException());
        await refresh;

        Assert.Multiple(() =>
        {
            Assert.That(host.Store.Get("hanging").State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(host.Store.Get("healthy"), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public async Task An_already_cancelled_token_leaves_every_plugin_denied()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var host = PluginTestHost.Create(
            new FakeExplorerPlugin(
                "cancels",
                gate: ExplorerPluginAccessGates.FromDelegate(static (_, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(ExplorerPluginAccess.Allowed);
                })));

        await host.Refresher.RefreshAsync(cts.Token);

        Assert.That(host.Store.Get("cancels").State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task A_gate_is_probed_with_its_own_bound_context()
    {
        var gateA = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Allowed);
        var gateB = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Denied);

        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("a", gate: gateA),
            new FakeExplorerPlugin("b", gate: gateB));

        await host.Refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(gateA.ObservedContext!.PluginId, Is.EqualTo("a"));
            Assert.That(gateB.ObservedContext!.PluginId, Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task RefreshAsync_over_an_empty_catalog_completes_and_writes_nothing()
    {
        var host = PluginTestHost.Create();

        await host.Refresher.RefreshAsync();

        Assert.That(host.Store.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task Single_plugin_refresh_probes_only_that_plugin()
    {
        var gateA = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Allowed);
        var gateB = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Allowed);

        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("a", gate: gateA),
            new FakeExplorerPlugin("b", gate: gateB));

        await host.Refresher.RefreshAsync("a");

        Assert.Multiple(() =>
        {
            Assert.That(gateA.ProbeCount, Is.EqualTo(1));
            Assert.That(gateB.ProbeCount, Is.Zero);
            Assert.That(host.Store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(host.Store.Get("b"), Is.EqualTo(ExplorerPluginAccess.Denied));
        });
    }

    [Test]
    public async Task Single_plugin_refresh_of_an_unknown_id_is_a_no_op()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", gate: ExplorerPluginAccessGates.Allowed));

        await host.Refresher.RefreshAsync("missing");

        Assert.That(host.Store.Snapshot(), Is.Empty);
    }

    [Test]
    public void Single_plugin_refresh_rejects_a_null_id()
    {
        var host = PluginTestHost.Create();

        Assert.That(() => host.Refresher.RefreshAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var host = PluginTestHost.Create();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerPluginAccessRefresher(null!, host.Store, host.Contexts),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginAccessRefresher(host.Catalog, null!, host.Contexts),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginAccessRefresher(host.Catalog, host.Store, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_plugin_that_faults_before_yielding_an_id_is_contained()
    {
        // The default catalog rejects a plugin this malformed at construction,
        // so the refresher is driven over a hand-rolled catalog to prove the
        // containment holds even when nothing upstream validated the set.
        var healthy = new FakeExplorerPlugin("healthy", gate: ExplorerPluginAccessGates.Allowed);
        var host = PluginTestHost.Create(healthy);
        var refresher = new ExplorerPluginAccessRefresher(
            new StubCatalog([new DescriptorFaultingPlugin(), healthy]),
            host.Store,
            host.Contexts);

        await refresher.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(host.Store.Get("healthy"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(host.Store.Snapshot(), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_stale_probe_does_not_overwrite_a_newer_decision()
    {
        // Two refreshes overlap - the shell fires one on an authentication change
        // and another when the connection reaches the cluster, both
        // fire-and-forget - and the older one's gate answers last. The decision
        // filed must be the one that was asked for last, not the one that
        // happened to complete last: otherwise a probe issued while the caller
        // was still signed in re-admits a plugin after the sign-out that denied
        // it.
        var gate = new SequencedExplorerPluginAccessGate();
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", gate: gate));

        var stale = host.Refresher.RefreshAsync();
        var fresh = host.Refresher.RefreshAsync();

        // The newer request answers first, then the older one answers with the
        // admission it resolved before the caller signed out.
        gate.Complete(1, ExplorerPluginAccess.Deny("signed out"));
        gate.Complete(0, ExplorerPluginAccess.Allowed);
        await Task.WhenAll(stale, fresh);

        Assert.That(
            host.Store.Get("a"),
            Is.EqualTo(ExplorerPluginAccess.Deny("signed out")),
            "the newest requested probe owns the decision, whatever order the probes complete in");
    }

    [Test]
    public async Task A_stale_probe_does_not_overwrite_a_newer_decision_for_a_single_plugin_refresh()
    {
        // The same ordering hazard across the two refresh entry points: a
        // whole-catalog refresh overlapping a targeted re-probe of one plugin.
        var gate = new SequencedExplorerPluginAccessGate();
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", gate: gate));

        var stale = host.Refresher.RefreshAsync();
        var fresh = host.Refresher.RefreshAsync("a");

        gate.Complete(1, ExplorerPluginAccess.Deny("revoked"));
        gate.Complete(0, ExplorerPluginAccess.Allowed);
        await Task.WhenAll(stale, fresh);

        Assert.That(host.Store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Deny("revoked")));
    }

    [Test]
    public async Task A_newer_probe_still_wins_when_it_completes_last()
    {
        // The ordering guard must not degrade into "first answer wins": when the
        // probes complete in request order the newest answer is still the one
        // filed.
        var gate = new SequencedExplorerPluginAccessGate();
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", gate: gate));

        var stale = host.Refresher.RefreshAsync();
        var fresh = host.Refresher.RefreshAsync();

        gate.Complete(0, ExplorerPluginAccess.Allowed);
        gate.Complete(1, ExplorerPluginAccess.Deny("revoked"));
        await Task.WhenAll(stale, fresh);

        Assert.That(host.Store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Deny("revoked")));
    }

    [Test]
    public async Task An_ordering_discarded_probe_does_not_hold_back_a_later_refresh()
    {
        // After a stale answer has been discarded, the plugin must still be
        // re-probeable: the guard orders decisions, it does not latch one.
        var gate = new SequencedExplorerPluginAccessGate();
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", gate: gate));

        var stale = host.Refresher.RefreshAsync();
        var fresh = host.Refresher.RefreshAsync();
        gate.Complete(1, ExplorerPluginAccess.Deny("revoked"));
        gate.Complete(0, ExplorerPluginAccess.Allowed);
        await Task.WhenAll(stale, fresh);

        var later = host.Refresher.RefreshAsync();
        gate.Complete(2, ExplorerPluginAccess.Allowed);
        await later;

        Assert.That(host.Store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
    }

    /// <summary>
    /// A gate that hands every probe its own pending completion source, so a test
    /// can answer overlapping probes in any order it likes without a clock.
    /// </summary>
    private sealed class SequencedExplorerPluginAccessGate : IExplorerPluginAccessGate
    {
        private readonly List<TaskCompletionSource<ExplorerPluginAccess>> _probes = [];

        /// <summary>Completes the <paramref name="index"/>th probe with <paramref name="access"/>.</summary>
        public void Complete(int index, ExplorerPluginAccess access)
        {
            TaskCompletionSource<ExplorerPluginAccess> probe;
            lock (_probes)
            {
                probe = _probes[index];
            }

            probe.SetResult(access);
        }

        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default)
        {
            var probe = new TaskCompletionSource<ExplorerPluginAccess>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_probes)
            {
                _probes.Add(probe);
            }

            return new ValueTask<ExplorerPluginAccess>(probe.Task);
        }
    }

    /// <summary>A catalog that reports exactly what it was handed, without validating it.</summary>
    private sealed class StubCatalog(IReadOnlyList<IExplorerPlugin> plugins) : IExplorerPluginCatalog
    {
        public IReadOnlyList<IExplorerPlugin> All => plugins;

        public IReadOnlyList<IExplorerPlugin> ForSurface(ExplorerPluginSurface surface) => plugins;

        public IReadOnlyList<IExplorerPlugin> ForSelection(ExplorerPluginSelectionKind kind) => plugins;

        public IExplorerPlugin? Find(string pluginId) => null;
    }

    /// <summary>A plugin whose descriptor cannot even be read.</summary>
    private sealed class DescriptorFaultingPlugin : IExplorerPlugin
    {
        public ExplorerPluginDescriptor Descriptor => throw new InvalidOperationException("descriptor exploded");

        public Type ViewType => typeof(DescriptorFaultingPlugin);

        public Type? DomainContract => null;

        public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
    }
}
