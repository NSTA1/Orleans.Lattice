using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression tests for issue #3993: with schema versioning registered, a tree that
/// has <b>no</b> version config must be unversioned end to end. The suite composes
/// the production registration (<c>AddLatticeSchemaVersioning</c>) over the real
/// version store, provider, write interceptor, value decoder and admin, backed by an
/// in-memory <c>sys-schema-version</c> tree that holds no config for the tree under
/// test, so the absent-key read goes through the real typed <c>ILattice.GetAsync</c>
/// path that used to surface <c>default(LatticeSchemaVersionConfig)</c> as a
/// non-null config and envelope every write at family 0 / version 0.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaVersioningUnconfiguredTreeTests
{
    private const string TreeId = "orders";

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static IServiceProvider BuildVersioningServices()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();
        builder.AddLatticeSchemaVersioning();

        // Every reserved schema tree resolves to its own empty in-memory lattice, so
        // the tree under test has no version config.
        var trees = new Dictionary<string, ILattice>(StringComparer.Ordinal);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(ci =>
            {
                var id = ci.ArgAt<string>(0);
                if (!trees.TryGetValue(id, out var lattice))
                {
                    lattice = InMemoryLatticeFake.Create(new SortedDictionary<string, byte[]>(StringComparer.Ordinal));
                    trees[id] = lattice;
                }

                return lattice;
            });
        builder.Services.AddSingleton(grainFactory);

        return builder.Services.BuildServiceProvider();
    }

    /// <summary>The bytes the write choke point persists for <paramref name="value"/> under <paramref name="decision"/>.</summary>
    private static byte[] StoredBytes(LatticeWriteDecision decision, byte[] value) =>
        decision.Kind == LatticeWriteDecisionKind.AcceptTransformed ? decision.TransformedValue! : value;

    [Test]
    public async Task Write_to_unconfigured_tree_stores_bytes_byte_identical_to_the_input()
    {
        var services = BuildVersioningServices();
        var interceptor = services.GetRequiredService<ILatticeWriteInterceptor>();
        var value = Utf8("{\"a\":1}");

        var decision = await interceptor.OnWriteAsync(new LatticeWriteRequest(TreeId, "k1", value, LatticeOperation.Write));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        Assert.That(decision.TransformedValue, Is.Null);
        var stored = StoredBytes(decision, value);
        Assert.That(stored, Is.EqualTo(Utf8("{\"a\":1}")));
        Assert.That(LatticeSchemaEnvelope.IsEnveloped(stored), Is.False);
    }

    [Test]
    public async Task Crdt_delta_to_unconfigured_tree_is_stored_byte_identical_to_the_input()
    {
        var services = BuildVersioningServices();
        var interceptor = services.GetRequiredService<ILatticeWriteInterceptor>();
        var delta = Utf8("crdt-delta");

        var decision = await interceptor.OnWriteAsync(new LatticeWriteRequest(TreeId, "k1", delta, LatticeOperation.CrdtApply));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        Assert.That(StoredBytes(decision, delta), Is.EqualTo(Utf8("crdt-delta")));
    }

    [Test]
    public async Task Written_value_on_unconfigured_tree_reads_back_unchanged()
    {
        var services = BuildVersioningServices();
        var interceptor = services.GetRequiredService<ILatticeWriteInterceptor>();
        var decoder = services.GetRequiredService<ILatticeValueDecoder>();
        var value = Utf8("{\"a\":1}");

        var decision = await interceptor.OnWriteAsync(new LatticeWriteRequest(TreeId, "k1", value, LatticeOperation.Write));
        var stored = StoredBytes(decision, value);
        var read = await decoder.DecodeAsync(TreeId, stored, CancellationToken.None);

        Assert.That(read, Is.SameAs(stored));
        Assert.That(read, Is.EqualTo(Utf8("{\"a\":1}")));
    }

    [Test]
    public async Task Read_of_a_self_describing_value_on_unconfigured_tree_returns_its_body_unchanged()
    {
        var services = BuildVersioningServices();
        var decoder = services.GetRequiredService<ILatticeValueDecoder>();
        var body = Utf8("{\"a\":1}");

        // A value that arrived already stamped (for example replicated from a peer on
        // which the tree is versioned under schema family 0) must decode to its own
        // body: with no config there is no target to judge it against.
        var stored = LatticeSchemaEnvelope.Encode(schemaId: 0, version: 1, body);

        var read = await decoder.DecodeAsync(TreeId, stored, CancellationToken.None);

        Assert.That(read, Is.EqualTo(body));
    }

    [Test]
    public async Task Version_admin_reports_no_config_for_unconfigured_tree()
    {
        var services = BuildVersioningServices();
        var admin = services.GetRequiredService<ILatticeSchemaVersionAdmin>();

        Assert.That(await admin.GetVersionConfigAsync(TreeId), Is.Null);
    }

    [Test]
    public async Task Advancing_the_target_of_an_unconfigured_tree_throws_and_writes_no_config()
    {
        var services = BuildVersioningServices();
        var admin = services.GetRequiredService<ILatticeSchemaVersionAdmin>();

        Assert.That(
            () => admin.AdvanceTargetVersionAsync(TreeId, 2),
            Throws.InvalidOperationException);
        Assert.That(await admin.GetVersionConfigAsync(TreeId), Is.Null);
    }

    [Test]
    public void Migrating_an_unconfigured_tree_throws()
    {
        var services = BuildVersioningServices();
        var admin = services.GetRequiredService<ILatticeSchemaVersionAdmin>();

        Assert.That(
            () => admin.MigrateToTargetVersionAsync(TreeId),
            Throws.InvalidOperationException);
    }
}
