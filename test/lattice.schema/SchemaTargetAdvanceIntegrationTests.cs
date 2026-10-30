using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end detectors for the contract that a version-config change rewrites no
/// stored value: advancing or clearing the target changes only the config, so every
/// stored value keeps the stamp it was written with and is upcast (or refused) at
/// read time from that stamp. Each test makes the stamp observable through the
/// decoding read, which fails explicitly for a value stamped at a version with no
/// path to the target, so a change that re-stamped or stripped stored values would
/// read back differently. The fixture's host declares versions 1 to 3 of
/// <see cref="SchemaVersionMigrationClusterFixture.SchemaId"/> with only a v2 to v3
/// hop.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaTargetAdvanceIntegrationTests
{
    private const uint SchemaId = SchemaVersionMigrationClusterFixture.SchemaId;

    private SchemaVersionMigrationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeSchemaVersionAdmin Versions => _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaVersionMigrationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static string Text(byte[]? value) => value is null ? string.Empty : Encoding.UTF8.GetString(value);

    [Test]
    public async Task AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read()
    {
        var treeId = $"advance-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 1));
        await tree.SetAsync("k", Utf8("{\"v\":1}"));

        await Versions.AdvanceTargetVersionAsync(treeId, 2);

        // Still stamped at v1, and the host declares no v1 to v2 hop.
        Assert.That(async () => await tree.GetAsync("k"), Throws.InstanceOf<NotSupportedException>());

        await _fixture.SiloServices.GetRequiredService<SchemaRuntimeRegistry>().RegisterAsync(
            treeId,
            new LatticeSchemaRuntimeRegistration
            {
                Upcasters = [new()
                {
                    SchemaId = SchemaId, FromVersion = 1, ToVersion = 2,
                    Transform = LatticeValueTransform.Passthrough(
                        LatticeValueTransform.SetMember("upcast", LatticeValueTransform.Const(LatticeConstant.Integer(2)))),
                }],
            });

        var read = Text(await tree.GetAsync("k"));

        Assert.That(read, Does.Contain("\"v\":1").And.Contain("\"upcast\":2"),
            "the v1 value is upcast at read time from the stamp it was written with");
    }

    [Test]
    public async Task ClearVersionConfigAsync_rewrites_no_stored_value_and_reads_return_the_stored_body()
    {
        var treeId = $"clear-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 2));
        await tree.SetAsync("k", Utf8("{\"v\":2}"));

        Assert.That(await Versions.ClearVersionConfigAsync(treeId), Is.True);

        Assert.That(Text(await tree.GetAsync("k")), Is.EqualTo("{\"v\":2}"),
            "an unversioned tree reads a stamped value as its stored body");

        // Opting back in below the value's stamp shows the stamp survived the clear:
        // a value stripped of its envelope would read back verbatim instead.
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 1));
        Assert.That(async () => await tree.GetAsync("k"), Throws.InstanceOf<NotSupportedException>());
    }
}
