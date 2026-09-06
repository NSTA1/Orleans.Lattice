using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests;

/// <summary>
/// End-to-end integration test proving that a <see cref="LatticeCredential"/>
/// stamped at the <see cref="ILattice"/> client edge via
/// <see cref="LatticeCredentialContext"/> is observable inside a downstream
/// grain via the Orleans <see cref="RequestContext"/>. The probe is a silo-side
/// <see cref="IMutationObserver"/> whose <c>OnMutationAsync</c> runs inside the
/// leaf grain's execution context, where it reads the ambient credential and
/// forwards it to the test process for assertion.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CredentialPropagationIntegrationTests
{
    private CredentialProbeClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CredentialProbeClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public void ResetAmbient()
    {
        LatticeCredentialContext.Current = null;
        CredentialProbeClusterFixture.Observed.Clear();
    }

    private ILattice NewTree() =>
        _fixture.Cluster.GrainFactory.GetGrain<ILattice>($"cred-{Guid.NewGuid():N}");

    [Test]
    public async Task Credential_set_at_edge_is_observable_inside_downstream_grain()
    {
        var tree = NewTree();
        var metadata = new Dictionary<string, string> { ["sub"] = "alice" };

        using (LatticeCredentialContext.Use("edge-token", "Bearer", "alice", metadata))
        {
            await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        var observed = await DequeueObservationAsync();
        Assert.That(observed, Is.Not.Null, "The downstream grain must have observed a credential.");
        Assert.That(observed!.Value.Token, Is.EqualTo("edge-token"));
        Assert.That(observed.Value.Scheme, Is.EqualTo("Bearer"));
        Assert.That(observed.Value.PrincipalId, Is.EqualTo("alice"));
        Assert.That(observed.Value.Metadata, Is.Not.Null);
        Assert.That(observed.Value.Metadata!["sub"], Is.EqualTo("alice"));
    }

    [Test]
    public async Task Absent_credential_is_null_inside_downstream_grain()
    {
        var tree = NewTree();

        // No credential scope entered - the write path must carry no credential.
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var observed = await DequeueObservationAsync();
        Assert.That(observed, Is.Null,
            "Absent an edge credential, the downstream grain must observe null (no allocation/cost).");
    }

    /// <summary>
    /// Waits at a hard barrier for the silo-side probe to record an observation
    /// for the write, then dequeues it.
    /// </summary>
    /// <remarks>
    /// The barrier and the dequeue are deliberately separate steps. The helper
    /// this replaces returned a nullable credential and fell through to
    /// <c>null</c> when its deadline elapsed - which is the very same value the
    /// probe records when it ran and observed no credential. "The observer never
    /// ran" and "the observer ran and saw null" were therefore indistinguishable,
    /// so each test could only ever report the other one's failure. Waiting for
    /// the observation to exist before reading it separates the two, and reports
    /// a missing observation where it actually happened.
    /// </remarks>
    private static async Task<LatticeCredential?> DequeueObservationAsync()
    {
        await TestPoll.UntilAsync(
            () => !CredentialProbeClusterFixture.Observed.IsEmpty,
            "the silo-side mutation observer to record an observation for the write",
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20));

        Assert.That(CredentialProbeClusterFixture.Observed.TryDequeue(out var observed), Is.True,
            "The observer must have run for the write.");
        return observed;
    }
}

/// <summary>
/// Test cluster fixture that registers a silo-side <see cref="IMutationObserver"/>
/// which captures the ambient <see cref="LatticeCredentialContext.Current"/>
/// seen inside the grain's execution context for each mutation, so the test
/// process can assert credential propagation across the client-to-silo hop.
/// </summary>
public sealed class CredentialProbeClusterFixture
{
    /// <summary>
    /// Process-global sink of credentials observed inside the grain (one entry
    /// per mutation the probe observer sees). Drained per test.
    /// </summary>
    public static readonly ConcurrentQueue<LatticeCredential?> Observed = new();

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IMutationObserver, CredentialCapturingObserver>();
        }
    }

    /// <summary>
    /// Silo-side observer that snapshots the ambient credential the grain saw
    /// (read straight off the propagated <see cref="RequestContext"/>) into the
    /// process-global <see cref="Observed"/> queue.
    /// </summary>
    internal sealed class CredentialCapturingObserver : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Observed.Enqueue(LatticeCredentialContext.Current);
            return Task.CompletedTask;
        }
    }
}
