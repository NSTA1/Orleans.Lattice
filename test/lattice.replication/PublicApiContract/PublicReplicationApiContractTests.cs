using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// End-to-end integration suite that pins the public
/// <c>Orleans.Lattice.Replication</c> API contract for a two-cluster
/// deployment. Every public type, interface, extension, accessor, and
/// option surfaced from the replication package is exercised here so
/// that any silent change to the wire shape, ship pipeline, encoder,
/// applier, cursor registry, dead-letter queue, GC, snapshot stream,
/// bootstrap state machine, or admin API surfaces as a test failure.
/// <para>
/// Tests are split across partial files by concern; this main file
/// holds the fixture wiring and shared helpers. Each test uses a
/// unique tree id so writes do not leak across tests, and tests that
/// need cross-cluster delivery rely on the fixture's
/// <see cref="LoopbackDeliveringTransport"/> bridging Site A and Site B
/// inside the same test process.
/// </para>
/// </summary>
[TestFixture]
[Category("API")]
public partial class PublicReplicationApiContractTests
{
    private PublicReplicationApiClusterFixture _fixture = null!;
    private int _treeIdCounter;

    /// <summary>The Site A test cluster.</summary>
    private TestCluster SiteA => _fixture.SiteA;

    /// <summary>The Site B test cluster.</summary>
    private TestCluster SiteB => _fixture.SiteB;

    /// <summary>Convenience accessor for the Site A cluster client.</summary>
    private IGrainFactory ClientA => _fixture.ClientA;

    /// <summary>Convenience accessor for the Site B cluster client.</summary>
    private IGrainFactory ClientB => _fixture.ClientB;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new PublicReplicationApiClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    /// <summary>UTF-8 encode for value bytes in tests.</summary>
    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>UTF-8 decode for value bytes in tests.</summary>
    private static string Str(byte[]? value) => value is null ? string.Empty : Encoding.UTF8.GetString(value);

    /// <summary>Key-value pair shorthand used by batch / bulk tests.</summary>
    private static KeyValuePair<string, byte[]> Kvp(string key, string value) =>
        new(key, Bytes(value));

    /// <summary>Allocates a unique tree id for the calling test.</summary>
    private string NextTreeId(string label)
    {
        ArgumentNullException.ThrowIfNull(label);
        var index = Interlocked.Increment(ref _treeIdCounter);
        return $"pubapi-{label}-{index:D4}";
    }

    /// <summary>Creates a freshly-registered, replicated tree on both sites.</summary>
    private Task<ILattice> CreateReplicatedTreeAsync(string treeId) =>
        _fixture.CreateReplicatedTreeAsync(treeId);
}
