using System.Text;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A dedicated Orleans TestingHost cluster for the atomic-action (saga / TCC)
/// coordinator (issue #1609). Unlike the shared <see cref="ClusterFixture"/>, this
/// fixture also calls <c>AddLatticeAtomicAction</c> to register the handler catalog
/// the coordinator resolves against, wiring a small set of deterministic test
/// handlers whose forward / compensate effects write observable markers into the
/// fixture's Lattice tree (<see cref="TreeId"/>) so a test can assert commit and
/// compensation through real serialization and a live activation.
/// </summary>
[Category("Integration")]
public sealed class AtomicActionClusterFixture
{
    /// <summary>The Lattice tree id the fixture's handlers and tree-write steps target.</summary>
    public const string TreeId = "aa-int-tree";

    /// <summary>A custom handler that writes a commit marker on forward and a rollback marker on compensate.</summary>
    public const string MarkHandler = "test.mark";

    /// <summary>A custom handler whose forward effect always faults, to trigger compensation.</summary>
    public const string FailForwardHandler = "test.fail-forward";

    /// <summary>A custom handler whose compensating effect always faults, to trigger CompensationFailed.</summary>
    public const string FailCompensateHandler = "test.fail-compensate";

    /// <summary>The byte a mark handler writes on a successful forward effect.</summary>
    public static readonly byte[] ForwardMarker = [1];

    /// <summary>The byte a mark handler writes when its compensating effect runs.</summary>
    public static readonly byte[] CompensateMarker = [2];

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

    /// <summary>Decodes a mark-handler args payload (a UTF-8 tree key).</summary>
    public static string DecodeKey(ReadOnlyMemory<byte> args) => Encoding.UTF8.GetString(args.Span);

    /// <summary>Encodes a mark-handler args payload (a UTF-8 tree key).</summary>
    public static byte[] EncodeKey(string key) => Encoding.UTF8.GetBytes(key);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeAtomicAction(handlers => handlers
                .AddHandler(
                    MarkHandler,
                    "v1",
                    async ctx =>
                    {
                        var tree = ctx.GrainFactory.GetGrain<ILattice>(TreeId);
                        await tree.SetAsync(DecodeKey(ctx.Args), ForwardMarker);
                    },
                    async ctx =>
                    {
                        var tree = ctx.GrainFactory.GetGrain<ILattice>(TreeId);
                        await tree.SetAsync(DecodeKey(ctx.Args), CompensateMarker);
                    })
                .AddHandler(
                    FailForwardHandler,
                    "v1",
                    _ => throw new InvalidOperationException("test forward fault"),
                    _ => Task.CompletedTask)
                .AddHandler(
                    FailCompensateHandler,
                    "v1",
                    async ctx =>
                    {
                        var tree = ctx.GrainFactory.GetGrain<ILattice>(TreeId);
                        await tree.SetAsync(DecodeKey(ctx.Args), ForwardMarker);
                    },
                    _ => throw new InvalidOperationException("test compensation fault")));
        }
    }
}
