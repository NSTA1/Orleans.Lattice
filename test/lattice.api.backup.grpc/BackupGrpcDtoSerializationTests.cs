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
    public void BackupSetCaptureRequestMessage_round_trips()
    {
        var original = new BackupSetCaptureRequestMessage
        {
            Name = "nightly-set",
            Scopes = new[]
            {
                BackupScopeSelector.WholeTree("orders"),
                BackupScopeSelector.WholeTree("customers"),
            },
            CrossTreeConsistent = true,
            PageSize = 512,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Name, Is.EqualTo("nightly-set"));
            Assert.That(copy.Scopes.Select(s => s.TreeId), Is.EqualTo(new[] { "orders", "customers" }));
            Assert.That(copy.CrossTreeConsistent, Is.True);
            Assert.That(copy.PageSize, Is.EqualTo(512));
        });
    }

    [Test]
    public void BackupSetCaptureResponse_round_trips_set_manifest_and_members()
    {
        var original = new BackupSetCaptureResponse
        {
            SetManifest = new BackupSetManifest(
                setId: "set-1",
                name: "nightly-set",
                createdAtUtc: DateTimeOffset.UnixEpoch,
                crossTreeConsistent: true,
                fence: null,
                memberBackupIds: new[] { "m0", "m1" }),
            Members = new[]
            {
                new BackupCaptureResponse { BackupId = "m0", Manifest = Manifest("m0") },
                new BackupCaptureResponse { BackupId = "m1", Manifest = Manifest("m1") },
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.SetManifest.SetId, Is.EqualTo("set-1"));
            Assert.That(copy.SetManifest.CrossTreeConsistent, Is.True);
            Assert.That(copy.SetManifest.MemberBackupIds, Is.EqualTo(new[] { "m0", "m1" }));
            Assert.That(copy.Members.Select(m => m.BackupId), Is.EqualTo(new[] { "m0", "m1" }));
        });
    }

    private static BackupManifest Manifest(string id)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: id,
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[] { new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope) },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) },
            baseBackupId: null);
    }

    [Test]
    public void BackupScheduleRequestMessage_round_trips()
    {
        var original = new BackupScheduleRequestMessage
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
            Incremental = true,
            IntervalTicks = TimeSpan.FromMinutes(90).Ticks,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Incremental, Is.True);
            Assert.That(copy.IntervalTicks, Is.EqualTo(TimeSpan.FromMinutes(90).Ticks));
        });
    }

    [Test]
    public void BackupScheduleResponse_round_trips()
    {
        var original = new BackupScheduleResponse
        {
            Scheduled = true,
            EffectiveIntervalTicks = TimeSpan.FromMinutes(1).Ticks,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Scheduled, Is.True);
            Assert.That(copy.EffectiveIntervalTicks, Is.EqualTo(TimeSpan.FromMinutes(1).Ticks));
        });
    }

    [Test]
    public void BackupCancelScheduleRequestMessage_round_trips()
    {
        var original = new BackupCancelScheduleRequestMessage
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
            Incremental = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Incremental, Is.True);
        });
    }

    [Test]
    public void BackupCancelScheduleResponse_round_trips()
    {
        Assert.That(RoundTrip(new BackupCancelScheduleResponse()), Is.Not.Null);
    }

    [Test]
    public void BackupScopeStatusResponse_round_trips_runtime_intervals()
    {
        var original = new BackupScopeStatusResponse
        {
            Found = true,
            Scope = BackupScopeSelector.WholeTree("orders"),
            FullScheduleRegistered = true,
            IncrementalScheduleRegistered = true,
            LastRunOutcome = BackupScopeRunOutcome.Success,
            ChainDepth = 2,
            RuntimeFullBackupIntervalTicks = TimeSpan.FromMinutes(20).Ticks,
            RuntimeIncrementalBackupIntervalTicks = TimeSpan.FromMinutes(45).Ticks,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.True);
            Assert.That(copy.Scope!.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.RuntimeFullBackupIntervalTicks, Is.EqualTo(TimeSpan.FromMinutes(20).Ticks));
            Assert.That(copy.RuntimeIncrementalBackupIntervalTicks, Is.EqualTo(TimeSpan.FromMinutes(45).Ticks));
        });
    }

    [Test]
    public void BackupScopeStatusRequestMessage_round_trips()
    {
        var copy = RoundTrip(new BackupScopeStatusRequestMessage
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
        });

        Assert.That(copy.Scope.TreeId, Is.EqualTo("orders"));
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
