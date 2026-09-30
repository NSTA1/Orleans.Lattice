using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusFanOut"/> (concurrent per-silo
/// reads folded into one page) and the idempotent
/// <see cref="ReplicationPeerStatusReadPath"/> registration.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusFanOutTests
{
    private static readonly ReplicationPeerStatusReadRequest AnyRead = new() { Limit = 10 };

    private static ReplicationPeerStatusRow Row(string tree, double contact = 1) =>
        new(tree, "east", ReplicationContactDirection.Outbound, 0, 0, 0, contact, 0);

    [Test]
    public async Task ReadAsync_reads_every_target_and_merges_the_answers()
    {
        var answers = new Dictionary<string, ReplicationPeerStatusRow[]>
        {
            ["silo-1"] = new[] { Row("b"), Row("orders", contact: 90) },
            ["silo-2"] = new[] { Row("a"), Row("orders", contact: 3) },
        };
        var asked = new List<(string Silo, ReplicationPeerStatusReadRequest Request)>();

        var rows = await ReplicationPeerStatusFanOut.ReadAsync(
            answers.Keys.ToArray(),
            (silo, request) =>
            {
                asked.Add((silo, request));
                return Task.FromResult(answers[silo]);
            },
            AnyRead,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(asked.Select(a => a.Silo), Is.EquivalentTo(new[] { "silo-1", "silo-2" }));
            Assert.That(asked.Select(a => a.Request), Has.All.SameAs(AnyRead));
            Assert.That(rows.Select(r => r.Tree), Is.EqualTo(new[] { "a", "b", "orders" }));
            Assert.That(rows.Single(r => r.Tree == "orders").LastContactSeconds, Is.EqualTo(3d));
        });
    }

    [Test]
    public async Task ReadAsync_with_no_targets_returns_no_rows_without_reading()
    {
        var rows = await ReplicationPeerStatusFanOut.ReadAsync(
            Array.Empty<string>(),
            (_, _) => throw new AssertionException("no target should be read"),
            AnyRead,
            CancellationToken.None);

        Assert.That(rows, Is.Empty);
    }

    [Test]
    public void ReadAsync_faults_when_any_target_fails()
    {
        Assert.That(
            async () => await ReplicationPeerStatusFanOut.ReadAsync(
                new[] { "ok", "down" },
                (silo, _) => silo == "down"
                    ? Task.FromException<ReplicationPeerStatusRow[]>(new InvalidOperationException("silo down"))
                    : Task.FromResult(new[] { Row("a") }),
                AnyRead,
                CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("silo down"));
    }

    [Test]
    public void ReadAsync_honours_cancellation_while_waiting()
    {
        using var cts = new CancellationTokenSource();
        var never = new TaskCompletionSource<ReplicationPeerStatusRow[]>();

        var read = ReplicationPeerStatusFanOut.ReadAsync(new[] { "slow" }, (_, _) => never.Task, AnyRead, cts.Token);
        cts.Cancel();

        Assert.That(async () => await read, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ReadAsync_already_cancelled_throws_before_reading()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await ReplicationPeerStatusFanOut.ReadAsync(
                new[] { "silo" },
                (_, _) => throw new AssertionException("no target should be read"),
                AnyRead,
                cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ReadAsync_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ReplicationPeerStatusFanOut.ReadAsync<string>(null!, (_, _) => Task.FromResult(Array.Empty<ReplicationPeerStatusRow>()), AnyRead, default),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await ReplicationPeerStatusFanOut.ReadAsync(new[] { "s" }, null!, AnyRead, default),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await ReplicationPeerStatusFanOut.ReadAsync(new[] { "s" }, (_, _) => Task.FromResult(Array.Empty<ReplicationPeerStatusRow>()), null!, default),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddReplicationPeerStatusReadPath_registers_once()
    {
        var builder = new FakeSiloBuilder();

        builder.AddReplicationPeerStatusReadPath();
        var afterFirst = builder.Services.Count;
        builder.AddReplicationPeerStatusReadPath();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count, Is.EqualTo(afterFirst), "a repeated call must add nothing");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(IReplicationPeerStatusReader)),
                Is.EqualTo(1));
            Assert.That(
                builder.Services.Single(d => d.ServiceType == typeof(IReplicationPeerStatusReader)).ImplementationType,
                Is.EqualTo(typeof(ClusterReplicationPeerStatusReader)));
        });
    }

    [Test]
    public void AddReplicationPeerStatusReadPath_keeps_a_reader_registered_first()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<IReplicationPeerStatusReader>(new StubReader());

        builder.AddReplicationPeerStatusReadPath();

        Assert.That(
            builder.Services.Count(d => d.ServiceType == typeof(IReplicationPeerStatusReader)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddReplicationPeerStatusReadPath_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddReplicationPeerStatusReadPath(), Throws.ArgumentNullException);
    }

    [Test]
    public void Reader_constructor_rejects_a_null_service_provider()
    {
        Assert.That(
            () => new ClusterReplicationPeerStatusReader(null!, null!),
            Throws.ArgumentNullException);
    }

    private sealed class StubReader : IReplicationPeerStatusReader
    {
        public Task<IReadOnlyList<ReplicationPeerStatusRow>> ReadAsync(
            ReplicationPeerStatusReadRequest request,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<ReplicationPeerStatusRow>>(Array.Empty<ReplicationPeerStatusRow>());
    }

    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
