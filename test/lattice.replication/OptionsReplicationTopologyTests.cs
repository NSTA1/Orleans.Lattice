using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="OptionsReplicationTopology"/>: the
/// default <see cref="IReplicationTopology"/> implementation that
/// projects <see cref="LatticeReplicationOptions.ReplicationPeers"/>
/// via <c>IOptionsMonitor.OnChange</c> diffs.
/// </summary>
[TestFixture]
public class OptionsReplicationTopologyTests
{
    /// <summary>
    /// Helper that builds an <see cref="IOptionsMonitor{T}"/> stub whose
    /// <c>CurrentValue</c> can be flipped via the returned setter and
    /// whose <c>OnChange</c> callback is captured for later invocation
    /// by the test body. The pattern mirrors the one used in
    /// <c>ReplicationMutationObserverTests</c>.
    /// </summary>
    private static (IOptionsMonitor<LatticeReplicationOptions> Monitor,
                    Action<LatticeReplicationOptions> SetCurrent,
                    Action<LatticeReplicationOptions, string?> Fire)
        CreateMonitor(LatticeReplicationOptions initial)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        Action<LatticeReplicationOptions, string?>? captured = null;
        var current = initial;
        monitor.CurrentValue.Returns(_ => current);
        monitor.Get(Arg.Any<string>()).Returns(_ => current);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(call =>
        {
            captured = call.Arg<Action<LatticeReplicationOptions, string?>>();
            return Substitute.For<IDisposable>();
        });

        void Set(LatticeReplicationOptions next) => current = next;
        void Fire(LatticeReplicationOptions opts, string? name)
        {
            if (captured is null) throw new InvalidOperationException("OnChange not subscribed yet.");
            captured(opts, name);
        }
        return (monitor, Set, Fire);
    }

    private static LatticeReplicationOptions Options(params string[] peers) => new()
    {
        ClusterId = "site-a",
        ReplicationPeers = peers,
    };

    [Test]
    public void CurrentPeers_returns_initial_snapshot_after_construction()
    {
        var (monitor, _, _) = CreateMonitor(Options("site-b", "site-c"));
        using var topology = new OptionsReplicationTopology(monitor);

        Assert.That(topology.CurrentPeers, Is.EquivalentTo(new[] { "site-b", "site-c" }));
    }

    [Test]
    public void CurrentPeers_filters_null_empty_and_whitespace_entries()
    {
        var raw = new[] { "site-b", "", "  ", null!, "site-b" };
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ReplicationPeers = raw };
        var (monitor, _, _) = CreateMonitor(opts);
        using var topology = new OptionsReplicationTopology(monitor);

        Assert.That(topology.CurrentPeers, Is.EquivalentTo(new[] { "site-b" }));
    }

    [Test]
    public void CurrentPeers_returns_empty_when_replication_peers_is_null()
    {
        var (monitor, _, _) = CreateMonitor(new LatticeReplicationOptions { ClusterId = "site-a" });
        using var topology = new OptionsReplicationTopology(monitor);

        Assert.That(topology.CurrentPeers, Is.Empty);
    }

    [Test]
    public void Subscribe_does_not_replay_initial_snapshot()
    {
        var (monitor, _, _) = CreateMonitor(Options("site-b", "site-c"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();

        using var _ = topology.Subscribe(events.Add);

        Assert.That(events, Is.Empty);
    }

    [Test]
    public void OnChange_emits_added_event_for_new_peer()
    {
        var (monitor, set, fire) = CreateMonitor(Options("site-b"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        using var _ = topology.Subscribe(events.Add);

        var next = Options("site-b", "site-c");
        set(next);
        fire(next, null);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0], Is.EqualTo(new PeerChanged("site-c", PeerChangeKind.Added)));
        Assert.That(topology.CurrentPeers, Is.EquivalentTo(new[] { "site-b", "site-c" }));
    }

    [Test]
    public void OnChange_emits_removed_event_for_withdrawn_peer()
    {
        var (monitor, set, fire) = CreateMonitor(Options("site-b", "site-c"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        using var _ = topology.Subscribe(events.Add);

        var next = Options("site-b");
        set(next);
        fire(next, null);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0], Is.EqualTo(new PeerChanged("site-c", PeerChangeKind.Removed)));
    }

    [Test]
    public void OnChange_emits_both_added_and_removed_in_one_reload()
    {
        var (monitor, set, fire) = CreateMonitor(Options("site-b"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        using var _ = topology.Subscribe(events.Add);

        var next = Options("site-c");
        set(next);
        fire(next, null);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.Select(e => e.PeerClusterId).ToHashSet(),
            Is.EquivalentTo(new[] { "site-b", "site-c" }));
        Assert.That(events.First(e => e.PeerClusterId == "site-c").Kind, Is.EqualTo(PeerChangeKind.Added));
        Assert.That(events.First(e => e.PeerClusterId == "site-b").Kind, Is.EqualTo(PeerChangeKind.Removed));
    }

    [Test]
    public void OnChange_suppresses_noop_reload()
    {
        var (monitor, set, fire) = CreateMonitor(Options("site-b"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        using var _ = topology.Subscribe(events.Add);

        var same = Options("site-b");
        set(same);
        fire(same, null);

        Assert.That(events, Is.Empty);
    }

    [Test]
    public void OnChange_ignores_named_options_reloads()
    {
        // Per-tree named options reloads must not produce peer-set
        // diffs - peer membership lives on the unnamed (cluster-wide)
        // instance only. The default name (string.Empty) is the
        // canonical "this is the cluster-wide instance" marker.
        var (monitor, set, fire) = CreateMonitor(Options("site-b"));
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        using var _ = topology.Subscribe(events.Add);

        var perTree = Options("site-c");
        set(perTree);
        fire(perTree, "my-tree");

        Assert.That(events, Is.Empty,
            "Named-options reload must not produce diff events.");
    }

    [Test]
    public void OnChange_fans_out_to_multiple_subscribers()
    {
        var (monitor, set, fire) = CreateMonitor(Options());
        using var topology = new OptionsReplicationTopology(monitor);
        var a = new List<PeerChanged>();
        var b = new List<PeerChanged>();
        using var _ = topology.Subscribe(a.Add);
        using var __ = topology.Subscribe(b.Add);

        var next = Options("site-b");
        set(next);
        fire(next, null);

        Assert.That(a, Has.Count.EqualTo(1));
        Assert.That(b, Has.Count.EqualTo(1));
    }

    [Test]
    public void OnChange_swallows_subscriber_exceptions()
    {
        var (monitor, set, fire) = CreateMonitor(Options());
        using var topology = new OptionsReplicationTopology(monitor);
        var observed = new List<PeerChanged>();
        using var _ = topology.Subscribe(_ => throw new InvalidOperationException("boom"));
        using var __ = topology.Subscribe(observed.Add);

        var next = Options("site-b");
        set(next);
        fire(next, null);

        Assert.That(observed, Has.Count.EqualTo(1),
            "Throwing subscriber must not poison the fan-out.");
    }

    [Test]
    public void Dispose_removes_subscriber_callbacks()
    {
        var (monitor, set, fire) = CreateMonitor(Options());
        using var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        var token = topology.Subscribe(events.Add);

        token.Dispose();
        var next = Options("site-b");
        set(next);
        fire(next, null);

        Assert.That(events, Is.Empty);
    }

    [Test]
    public void Subscriber_dispose_is_idempotent()
    {
        var (monitor, _, _) = CreateMonitor(Options());
        using var topology = new OptionsReplicationTopology(monitor);

        var token = topology.Subscribe(_ => { });
        token.Dispose();
        Assert.DoesNotThrow(() => token.Dispose());
    }

    [Test]
    public void Topology_dispose_clears_subscribers_and_throws_on_subscribe()
    {
        var (monitor, _, _) = CreateMonitor(Options());
        var topology = new OptionsReplicationTopology(monitor);

        topology.Dispose();

        Assert.Throws<ObjectDisposedException>(() => topology.Subscribe(_ => { }));
    }

    [Test]
    public void Topology_dispose_disposes_options_monitor_change_subscription()
    {
        // The OnChange handle returned by IOptionsMonitor must be
        // disposed when the topology is disposed; otherwise the
        // monitor keeps the callback rooted (and a re-registered
        // topology on the same monitor would receive duplicate
        // diff events).
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var changeHandle = Substitute.For<IDisposable>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "site-a" });
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "site-a" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(changeHandle);
        var topology = new OptionsReplicationTopology(monitor);

        topology.Dispose();

        changeHandle.Received(1).Dispose();
    }

    [Test]
    public void Topology_dispose_is_idempotent()
    {
        var (monitor, _, _) = CreateMonitor(Options());
        var topology = new OptionsReplicationTopology(monitor);

        topology.Dispose();
        Assert.DoesNotThrow(() => topology.Dispose());
    }

    [Test]
    public void OnChange_after_dispose_is_a_no_op()
    {
        // Disposing the topology must short-circuit Reconcile so a
        // late-arriving monitor reload (e.g. config-system reload
        // racing the host shutdown) does not invoke subscribers
        // whose subscriber-side disposables are already discarded.
        var (monitor, set, fire) = CreateMonitor(Options("site-b"));
        var topology = new OptionsReplicationTopology(monitor);
        var events = new List<PeerChanged>();
        topology.Subscribe(events.Add);

        topology.Dispose();

        var next = Options("site-b", "site-c");
        set(next);
        Assert.DoesNotThrow(() => fire(next, null));
        Assert.That(events, Is.Empty);
    }

    [Test]
    public void Subscribe_throws_when_callback_is_null()
    {
        var (monitor, _, _) = CreateMonitor(Options());
        using var topology = new OptionsReplicationTopology(monitor);

        Assert.Throws<ArgumentNullException>(() => topology.Subscribe(null!));
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        Assert.Throws<ArgumentNullException>(() => new OptionsReplicationTopology(null!));
    }
}
