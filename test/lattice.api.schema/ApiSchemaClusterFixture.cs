using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Schema;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, schema
/// enforcement and versioning, and the schema control-API add-on, exposing the
/// live facade so its accept-then-poll operations (#4123) can be driven end to
/// end. An outgoing-call filter can hold a remediation's first destination write
/// for an armed tree, pinning the run inside its build phase for as long as a test
/// chooses, without timing.
/// </summary>
public sealed class ApiSchemaClusterFixture
{
    /// <summary>The schema-family id the fixture's versioning registry declares, with versions 1 to 3.</summary>
    public const uint SchemaId = 11;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side schema control facade under test.</summary>
    internal LatticeSchemaControl Control => SiloServices.GetRequiredService<LatticeSchemaControl>();

    /// <summary>Deploys the single-silo cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>
    /// Builds a facade over the silo's live schema planes but with the supplied
    /// gate and, optionally, tenant resolver, so the fail-closed and tenant-scoped
    /// paths can be driven.
    /// </summary>
    internal LatticeSchemaControl CreateControlWith(ILatticeAccessGate gate, ITenantContextResolver? tenantResolver = null) =>
        new(
            SiloServices.GetRequiredService<ILatticeSchemaAdmin>(),
            SiloServices.GetRequiredService<ILatticeSchemaRemediationAdmin>(),
            SiloServices.GetRequiredService<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(gate),
            Options.Create(new LatticeApiSchemaOptions()),
            SiloServices,
            tenantResolver ?? new DefaultTenantContextResolver());

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        BuildWriteGate.ReleaseAll();
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning(registry => registry
                .AddSchema(SchemaId, 1, "ops-v1")
                .AddSchema(SchemaId, 2, "ops-v2")
                .AddSchema(SchemaId, 3, "ops-v3")
                .AddUpcaster(SchemaId, 1, 2, LatticeValueTransform.Passthrough())
                .AddUpcaster(SchemaId, 2, 3, LatticeValueTransform.Passthrough()));
            siloBuilder.AddLatticeSchemaApi();
            siloBuilder.AddOutgoingGrainCallFilter<BuildWriteGate>();
        }
    }
}

/// <summary>
/// Holds the first <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
/// a remediation makes into an armed tree's destination
/// (<c>{treeId}/remediated/{suffix}</c>) before it is sent, so no response timeout
/// ends the hold, keeping the run inside its build phase until the test releases it.
/// </summary>
internal sealed class BuildWriteGate : IOutgoingGrainCallFilter
{
    private const string DestinationInfix = "/remediated/";

    private static readonly ConcurrentDictionary<string, WriteHold> Holds = new(StringComparer.Ordinal);

    /// <summary>Arms a hold for <paramref name="treeId"/>.</summary>
    internal static WriteHold Arm(string treeId) => Holds.GetOrAdd(treeId, static _ => new WriteHold());

    /// <summary>Releases every hold.</summary>
    internal static void ReleaseAll()
    {
        foreach (var hold in Holds.Values) hold.Release.TrySetResult();
    }

    /// <inheritdoc />
    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        if (context.InterfaceMethod?.DeclaringType == typeof(ILattice)
            && context.MethodName == nameof(ILattice.SetAsync)
            && context.TargetId.Key.ToString() is { } key
            && key.IndexOf(DestinationInfix, StringComparison.Ordinal) is var at and > 0
            && Holds.TryGetValue(key[..at], out var hold))
        {
            hold.Entered.TrySetResult();
            await hold.Release.Task;
        }

        await context.Invoke();
    }
}

/// <summary>One armed hold: entered once the run reaches it, released by the test.</summary>
internal sealed class WriteHold
{
    /// <summary>Completes when the held write is reached.</summary>
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completed by the test to let the held write through.</summary>
    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
