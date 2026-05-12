using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// End-to-end integration suite that pins the public <see cref="ILattice"/>
/// API contract for a single-cluster deployment. Every public method,
/// extension, accessor, and ambient context surfaced from
/// <c>Orleans.Lattice</c> is exercised here so that any silent change to
/// the wire shape, mutation pipeline, or activation-time WAL replay path
/// surfaces as a test failure.
/// <para>
/// Tests are split across partial files by concern; this main file holds
/// the fixture wiring and shared helpers. Each test uses a unique tree
/// id so writes do not leak across tests.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("API")]
public partial class PublicApiContractTests
{
    private PublicApiContractClusterFixture _fixture = null!;

    /// <summary>The currently-active test cluster (set per <see cref="OneTimeSetUp"/>; reset on cluster restart).</summary>
    private TestCluster Cluster => _fixture.Cluster;

    /// <summary>Convenience accessor for the cluster client.</summary>
    private IGrainFactory Client => _fixture.Client;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new PublicApiContractClusterFixture();
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

    /// <summary>Resolves the default tree by id.</summary>
    private ILattice Tree(string treeId) => _fixture.GetTree(treeId);
}
