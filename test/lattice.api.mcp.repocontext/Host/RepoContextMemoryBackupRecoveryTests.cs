using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Fakes;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Criterion 5 of issue #2602, and the criterion the other seven serve: agent
/// memory written through the MCP tools survives the <b>total destruction of the
/// store</b> and comes back with its content intact.
/// <para>
/// A backup that has never been restored is not a backup. Everything else in
/// this issue - the dedicated sink, the bind mount, the cadence, the health
/// signal - is machinery in service of one question, which is whether the
/// entries come back. So this fixture answers that question the only way it can
/// be answered: it writes real entries over the real MCP protocol, captures
/// them, <b>disposes the entire cluster</b> (the harness uses in-memory grain
/// storage, so disposal destroys every tree exactly as
/// <c>docker compose down -v</c> destroyed the real one), stands a brand new
/// cluster up against the same sink, restores, and reads the entries back
/// through the ordinary read tool.
/// </para>
/// <para>
/// This is deliberately stronger than the fixtures in <c>test/lattice.backup</c>,
/// which "destroy" by restoring into a different tree id. That never removes the
/// original data, so it cannot distinguish a restore that worked from a read
/// that was served by data which was never lost.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: co-hosts two successive real Orleans silos and
/// drives the full MCP handshake against each.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextMemoryBackupRecoveryTests
{
    private const string RepoId = "backup-repo";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>The entries written before the disaster, and expected back after it.</summary>
    private static readonly (string Topic, string Id, string Title, string Body)[] Entries =
    [
        ("decisions", "d1", "Backups go to a dedicated sink",
            "The in-cluster sink stores backups inside the store they protect."),
        ("gotchas", "g1", "down -v removes every project volume",
            "A named volume for the sink is destroyed by the gesture it exists to survive."),
        ("conventions", "c1", "Assert on the resolved artefact",
            "Structural properties are asserted structurally, not described in prose."),
        ("glossary", "t1", "Cold restore",
            "Resolves a manifest from the sink alone, never from the catalog."),
    ];

    private static async Task<RepoContextMcpHarness> StartAsync(SharedBackupSink sink, CancellationToken ct) =>
        await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureSilo = silo =>
                {
                    silo.AddLatticeBackup();

                    // Replaces the in-cluster default sink, exactly as
                    // AddLatticeBackupAzureBlob does in the container.
                    silo.Services.Replace(ServiceDescriptor.Singleton<ILatticeBackupSink>(sink));
                },
            },
            ct);

    private async Task<JsonElement> CallAsync(McpClient client, string tool, Dictionary<string, object?> args)
    {
        var result = await client.CallToolAsync(tool, args, cancellationToken: Ct);
        return result.RequireStructuredContent();
    }

    private async Task WriteEntriesAsync(McpClient client)
    {
        foreach (var (topic, id, title, body) in Entries)
        {
            await CallAsync(client, "repocontext_remember", new()
            {
                ["repoId"] = RepoId,
                ["topic"] = topic,
                ["id"] = id,
                ["title"] = title,
                ["body"] = body,
            });
        }
    }

    private async Task AssertEntriesReadableAsync(McpClient client, string because)
    {
        foreach (var (topic, id, title, body) in Entries)
        {
            var recalled = await CallAsync(client, "repocontext_recall", new()
            {
                ["key"] = RepoContextKeys.Memory(RepoId, topic, id),
            });

            Assert.That(
                recalled.GetProperty("exists").GetBoolean(),
                Is.True,
                $"{because}: entry '{topic}/{id}' is absent.");

            var fields = recalled.GetProperty("fields");
            Assert.That(fields.GetProperty("title").GetString(), Is.EqualTo(title), because);
            Assert.That(fields.GetProperty("body").GetString(), Is.EqualTo(body), because);
        }
    }

    [Test]
    public async Task Agent_memory_written_before_the_store_is_destroyed_is_readable_after_a_cold_restore()
    {
        var sinkId = $"recovery-{Guid.NewGuid():N}";
        var sink = new SharedBackupSink(sinkId);
        string backupId;

        try
        {
            // -- Before the disaster ------------------------------------------
            await using (var original = await StartAsync(sink, Ct))
            {
                await using var client = await original.ConnectAsync(Ct);
                await WriteEntriesAsync(client);
                await AssertEntriesReadableAsync(client, "the entries were just written");

                var scheduler = original.Services.GetRequiredService<ILatticeBackupScheduler>();
                var captured = await scheduler.TriggerFullBackupAsync(RepoContextBackup.MemoryScope);

                Assert.That(captured, Is.Not.Null, "the full capture returned no backup id");
                backupId = captured!;

                // Criterion 4: the backup names the agent-memory tree, and it
                // describes entries. A capture over an empty or wrongly-scoped
                // selection also "succeeds", so success is not what is asserted.
                var manifest = await sink.ReadManifestAsync(backupId, Ct);
                Assert.That(manifest, Is.Not.Null, "the sink holds no manifest for the captured backup");
                Assert.That(
                    manifest!.Scope.TreeId,
                    Is.EqualTo(RepoContextHostTrees.Memory),
                    "the capture must be scoped to the agent-memory tree by name");
                Assert.That(
                    manifest.KeyDescriptors.Count,
                    Is.GreaterThanOrEqualTo(Entries.Length),
                    "the manifest must describe at least the entries that were written; a manifest "
                        + "describing zero entries is a successful backup that protects nothing");
                Assert.That(manifest.Kind, Is.EqualTo(BackupKind.Full));
            }

            // -- The disaster --------------------------------------------------
            // The harness disposed above owned every tree in in-memory grain
            // storage, so this is the whole store gone: the same loss a
            // 'docker compose down -v' inflicted on the real deployment. Only the
            // sink survives, because only the sink was never owned by that cluster.

            // -- After the disaster --------------------------------------------
            await using var replacement = await StartAsync(sink, Ct);
            await using var freshClient = await replacement.ConnectAsync(Ct);

            // The entries really are gone from the new cluster before the restore.
            // Without this the test could pass on data that was never destroyed.
            var beforeRestore = await CallAsync(freshClient, "repocontext_recall", new()
            {
                ["key"] = RepoContextKeys.Memory(RepoId, Entries[0].Topic, Entries[0].Id),
            });
            Assert.That(
                beforeRestore.GetProperty("exists").GetBoolean(),
                Is.False,
                "the replacement cluster must start empty, or this fixture proves nothing about recovery");

            var restore = replacement.Services.GetRequiredService<ILatticeBackupColdRestoreService>();
            var result = await restore.ColdRestoreAsync(
                new LatticeRestoreRequest(backupId, RepoContextHostTrees.Memory),
                Ct);

            Assert.That(result.EntriesApplied, Is.GreaterThanOrEqualTo(Entries.Length));

            await AssertEntriesReadableAsync(freshClient, "the entries were restored from the surviving sink");
        }
        finally
        {
            SharedBackupSink.Discard(sinkId);
        }
    }

    [Test]
    public async Task The_backup_catalog_does_not_survive_the_store_so_the_cold_path_is_the_only_one_that_works()
    {
        // This is why the container restores through ILatticeBackupColdRestoreService
        // and not ILatticeBackupRestoreService. The catalog dogfoods the reserved
        // 'sys-backup-catalog' Lattice tree, which lives inside the store being
        // protected - so the ordinary restore path, which resolves a manifest from
        // the catalog, cannot find the backup in exactly the disaster backups exist
        // for. Asserting it here keeps that from being re-discovered the hard way,
        // during an incident, by someone simplifying the host to the obvious call.
        var sinkId = $"catalog-{Guid.NewGuid():N}";
        var sink = new SharedBackupSink(sinkId);

        try
        {
            string backupId;
            await using (var original = await StartAsync(sink, Ct))
            {
                await using var client = await original.ConnectAsync(Ct);
                await WriteEntriesAsync(client);

                var scheduler = original.Services.GetRequiredService<ILatticeBackupScheduler>();
                backupId = (await scheduler.TriggerFullBackupAsync(RepoContextBackup.MemoryScope))!;
                Assert.That(backupId, Is.Not.Null);

                var catalog = original.Services.GetRequiredService<ILatticeBackupCatalogStore>();
                Assert.That(
                    await catalog.GetAsync(backupId, Ct),
                    Is.Not.Null,
                    "the catalog holds the manifest while the store that hosts it is alive");
            }

            await using var replacement = await StartAsync(sink, Ct);

            var freshCatalog = replacement.Services.GetRequiredService<ILatticeBackupCatalogStore>();
            Assert.That(
                await freshCatalog.GetAsync(backupId, Ct),
                Is.Null,
                "the catalog died with the store, so a catalog-resolving restore cannot find the backup");

            // The sink, which was never owned by that cluster, still has it.
            Assert.That(
                await sink.ReadManifestAsync(backupId, Ct),
                Is.Not.Null,
                "the surviving sink is the only place the manifest can be resolved from after the loss");
        }
        finally
        {
            SharedBackupSink.Discard(sinkId);
        }
    }

    [Test]
    public async Task The_sink_can_be_enumerated_after_the_loss_so_an_operator_can_discover_what_to_restore()
    {
        // After the store is gone the operator has no catalog to consult, so the
        // restorable ids have to be discoverable from the sink itself. The host
        // reports this at startup for exactly this reason.
        var sinkId = $"inventory-{Guid.NewGuid():N}";
        var sink = new SharedBackupSink(sinkId);

        try
        {
            await using (var original = await StartAsync(sink, Ct))
            {
                await using var client = await original.ConnectAsync(Ct);
                await WriteEntriesAsync(client);

                var scheduler = original.Services.GetRequiredService<ILatticeBackupScheduler>();
                Assert.That(await scheduler.TriggerFullBackupAsync(RepoContextBackup.MemoryScope), Is.Not.Null);
            }

            var manifests = new List<BackupManifest>();
            await foreach (var manifest in sink.ListManifestsAsync(Ct))
            {
                manifests.Add(manifest);
            }

            Assert.That(manifests, Is.Not.Empty);
            Assert.That(
                manifests.Where(m => m.Scope.TreeId == RepoContextHostTrees.Memory).ToList(),
                Is.Not.Empty,
                "the surviving sink must let an operator find the agent-memory backups by tree name");
        }
        finally
        {
            SharedBackupSink.Discard(sinkId);
        }
    }
}
