using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Lattice.Backup;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent across the wire, and asserts alias hygiene:
/// every gRPC wire message carries a unique <c>[Alias]</c> drawn from the
/// <see cref="GrpcBackupTypeAliases"/> registry under the reserved
/// <c>oibg.</c> prefix. The transport-agnostic facade DTOs are covered in the
/// <c>Orleans.Lattice.Api.Backup</c> test project; this fixture covers the
/// gRPC-only envelopes.
/// </summary>
[TestFixture]
public sealed class BackupGrpcDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void BackupCaptureRequestMessage_round_trips()
    {
        var original = new BackupCaptureRequestMessage
        {
            Name = "nightly",
            Scope = BackupScopeSelector.WholeTree("orders"),
            PageSize = 512,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Name, Is.EqualTo("nightly"));
            Assert.That(copy.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.PageSize, Is.EqualTo(512));
        });
    }

    [Test]
    public void BackupIncrementalCaptureRequestMessage_round_trips()
    {
        var original = new BackupIncrementalCaptureRequestMessage
        {
            Name = "incr",
            Scope = BackupScopeSelector.WholeTree("orders"),
            BaseBackupId = "base-id",
            PageSize = 256,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.BaseBackupId, Is.EqualTo("base-id"));
            Assert.That(copy.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.PageSize, Is.EqualTo(256));
        });
    }

    [Test]
    public void BackupChainResponse_round_trips_when_not_found()
    {
        var original = new BackupChainResponse { Found = false };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.False);
            Assert.That(copy.Manifest, Is.Null);
            Assert.That(copy.ChainBackupIds, Is.Empty);
        });
    }

    [Test]
    public void BackupDeleteResponse_round_trips()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(new BackupDeleteResponse { Deleted = true }).Deleted, Is.True);
            Assert.That(RoundTrip(new BackupDeleteResponse { Deleted = false }).Deleted, Is.False);
        });
    }

    [Test]
    public void RestoreRequestMessage_round_trips_with_optional_fields()
    {
        var original = new RestoreRequestMessage
        {
            BackupId = "backup-id",
            TargetTreeId = "target",
            Scope = BackupScopeSelector.WholeTree("orders"),
            Mode = LatticeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ApplyBatchSize = 128,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.BackupId, Is.EqualTo("backup-id"));
            Assert.That(copy.TargetTreeId, Is.EqualTo("target"));
            Assert.That(copy.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(copy.OperationId, Is.EqualTo("op-1"));
            Assert.That(copy.ApplyBatchSize, Is.EqualTo(128));
        });
    }

    [Test]
    public void RestoreResponse_round_trips_all_fields()
    {
        var original = new RestoreResponse
        {
            BackupId = "backup-id",
            TargetTreeId = "target",
            Mode = LatticeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = new[] { "base", "incr" },
            EntriesApplied = 4242,
            ShadowPhysicalTreeId = "shadow",
            PreviousPhysicalTreeId = "previous",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.ManifestChain, Is.EqualTo(new[] { "base", "incr" }));
            Assert.That(copy.EntriesApplied, Is.EqualTo(4242));
            Assert.That(copy.ShadowPhysicalTreeId, Is.EqualTo("shadow"));
            Assert.That(copy.PreviousPhysicalTreeId, Is.EqualTo("previous"));
        });
    }

    [Test]
    public void ArtifactChunk_round_trips()
    {
        var copy = RoundTrip(new ArtifactChunk { Data = new byte[] { 9, 8, 7 } });

        Assert.That(copy.Data, Is.EqualTo(new byte[] { 9, 8, 7 }));
    }

    [Test]
    public void ArtifactExportRequest_round_trips()
    {
        var original = new ArtifactExportRequest { BackupId = "b", ArtifactId = "a" };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.BackupId, Is.EqualTo("b"));
            Assert.That(copy.ArtifactId, Is.EqualTo("a"));
        });
    }

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcBackupTypeAliases.AliasPrefix, Is.EqualTo("oibg."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcBackupTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcBackupTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<AliasAttribute>()?.Alias
                is { } alias && alias.StartsWith(GrpcBackupTypeAliases.AliasPrefix, StringComparison.Ordinal))
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()!.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcBackupTypeAliases.");
                Assert.That(seen.Add(alias), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        typeof(GrpcBackupTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcBackupTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
