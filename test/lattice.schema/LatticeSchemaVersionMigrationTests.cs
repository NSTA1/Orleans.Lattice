using System.Linq;
using System.Text;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionMigration"/>: the pure, per-value
/// re-stamping step of an eager schema-version migration. Cover legacy un-stamped
/// stamping, idempotent at-target pass-through, the upcast-and-re-envelope path, and
/// the two abort surfaces (un-upcastable value, null value).
/// </summary>
public class LatticeSchemaVersionMigrationTests
{
    private const uint SchemaId = 7;

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static byte[] Env(uint version, string body) =>
        LatticeSchemaEnvelope.Encode(SchemaId, version, Utf8(body));

    private static ILatticeSchemaRegistry Registry(Func<byte[], byte[]>? bodyMap = null)
    {
        var registry = Substitute.For<ILatticeSchemaRegistry>();
        registry.Upcast(SchemaId, Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<byte[]>())
            .Returns(ci =>
            {
                var from = (uint)ci[1];
                var to = (uint)ci[2];
                var body = (byte[])ci[3];
                if (to <= from)
                {
                    throw new NotSupportedException($"No downcast from v{from} to v{to}.");
                }

                return bodyMap is null ? body : bodyMap(body);
            });
        return registry;
    }

    [Test]
    public void Migrate_legacy_unstamped_value_is_stamped_at_target_without_upcast()
    {
        var registry = Registry();
        var body = Utf8("{\"a\":1}");

        var result = LatticeSchemaVersionMigration.Migrate(body, SchemaId, 3, registry);

        LatticeSchemaEnvelope.TryReadHeader(result, out var schemaId, out var version);
        Assert.That(schemaId, Is.EqualTo(SchemaId));
        Assert.That(version, Is.EqualTo(3u));
        Assert.That(LatticeSchemaEnvelope.StripToBody(result).SequenceEqual(body), Is.True);
        registry.DidNotReceive().Upcast(Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<byte[]>());
    }

    [Test]
    public void Migrate_value_already_at_target_is_returned_unchanged()
    {
        var registry = Registry();
        var value = Env(2, "{\"a\":1}");

        var result = LatticeSchemaVersionMigration.Migrate(value, SchemaId, 2, registry);

        Assert.That(ReferenceEquals(result, value), Is.True);
        registry.DidNotReceive().Upcast(Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<byte[]>());
    }

    [Test]
    public void Migrate_below_target_value_is_upcast_and_reenveloped_at_target()
    {
        var registry = Registry(_ => Utf8("{\"a\":1,\"v\":2}"));
        var value = Env(1, "{\"a\":1}");

        var result = LatticeSchemaVersionMigration.Migrate(value, SchemaId, 2, registry);

        LatticeSchemaEnvelope.TryReadHeader(result, out var schemaId, out var version);
        Assert.That(schemaId, Is.EqualTo(SchemaId));
        Assert.That(version, Is.EqualTo(2u));
        Assert.That(LatticeSchemaEnvelope.StripToBody(result).SequenceEqual(Utf8("{\"a\":1,\"v\":2}")), Is.True);
        registry.Received(1).Upcast(SchemaId, 1, 2, Arg.Any<byte[]>());
    }

    [Test]
    public void Migrate_newer_than_target_value_throws_not_supported()
    {
        var registry = Registry();
        var value = Env(3, "{\"a\":1}");

        Assert.That(
            () => LatticeSchemaVersionMigration.Migrate(value, SchemaId, 2, registry),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Migrate_null_value_throws_invalid_operation()
    {
        var registry = Registry();

        Assert.That(
            () => LatticeSchemaVersionMigration.Migrate(null!, SchemaId, 2, registry),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Migrate_null_registry_throws_argument_null()
    {
        Assert.That(
            () => LatticeSchemaVersionMigration.Migrate(Env(1, "{}"), SchemaId, 2, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
