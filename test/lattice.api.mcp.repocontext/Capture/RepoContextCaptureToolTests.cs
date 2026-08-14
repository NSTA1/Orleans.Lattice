using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// End-to-end tests for the repository-context capture and maintenance tools over
/// the real MCP protocol via <see cref="RepoContextMcpHarness"/>: fail-closed
/// write gating for <c>repocontext_remember</c>, <c>_update</c>, and <c>_forget</c>
/// across every auth posture; the read tools (<c>repocontext_recall</c>,
/// <c>_scan</c>, <c>_list_topics</c>) offered to any authorized reader; ordered,
/// paged scans with a continuation token; CRDT-merge update semantics; and both
/// forget modes (hard delete and soft time-to-live lapse).
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an
/// in-process MCP server and drives the full streamable-HTTP handshake, so it is
/// excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextCaptureToolTests
{
    private const string RepoId = "capture-repo";

    private static readonly string[] WriteToolNames =
        ["repocontext_remember", "repocontext_update", "repocontext_forget"];

    private static readonly string[] ReadToolNames =
        ["repocontext_recall", "repocontext_scan", "repocontext_list_topics", "repocontext_neighbors"];

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task<RepoContextMcpHarness> StartAsync(RepoContextMcpAuthPosture posture, CancellationToken ct)
        => await RepoContextMcpHarness.StartAsync(new RepoContextMcpHarnessOptions { Posture = posture }, ct);

    private async Task<JsonElement> CallAsync(McpClient client, string tool, Dictionary<string, object?> args)
    {
        var result = await client.CallToolAsync(tool, args, cancellationToken: Ct);
        return result.RequireStructuredContent();
    }

    // -- Authorization gating --------------------------------------------------

    [TestCaseSource(nameof(WriteToolNames))]
    public async Task Writer_is_offered_each_write_tool(string toolName)
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(toolName));
    }

    [TestCaseSource(nameof(WriteToolNames))]
    public async Task Reader_is_never_offered_a_write_tool(string toolName)
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Not.Contain(toolName),
            "A reader (no write opt-in) must be offered none of the mutating capture tools.");
    }

    [TestCaseSource(nameof(WriteToolNames))]
    public async Task Reader_calling_a_write_tool_is_rejected(string toolName)
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        Assert.That(
            () => client.CallToolAsync(toolName, new Dictionary<string, object?>(), cancellationToken: Ct).AsTask(),
            Throws.InstanceOf<McpException>(),
            "A reader is denied a mutating tool it was never offered.");
    }

    [TestCaseSource(nameof(ReadToolNames))]
    public async Task Reader_is_offered_each_read_tool(string toolName)
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(toolName),
            "A reader is offered the whole read-only capture surface.");
    }

    [TestCaseSource(nameof(ReadToolNames))]
    public async Task Writer_is_offered_each_read_tool(string toolName)
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(toolName));
    }

    [Test]
    public async Task Unauthenticated_caller_sees_no_tools_and_is_denied_writes()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Unauthenticated, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Is.Empty, "A fail-closed session is offered no tools at all.");

        foreach (var toolName in WriteToolNames.Concat(ReadToolNames))
        {
            Assert.That(
                () => client.CallToolAsync(toolName, new Dictionary<string, object?>(), cancellationToken: Ct).AsTask(),
                Throws.InstanceOf<McpException>(),
                $"An unauthenticated caller is denied '{toolName}' at the protocol layer.");
        }
    }

    // -- Remember lifecycle ----------------------------------------------------

    [Test]
    public async Task Remember_creates_then_updates_a_memory_entry()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var created = await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "decisions",
            ["id"] = "d1",
            ["kind"] = "Decision",
            ["title"] = "Adopt CRDTs",
        });

        Assert.Multiple(() =>
        {
            Assert.That(created.GetProperty("created").GetBoolean(), Is.True);
            Assert.That(created.GetProperty("id").GetString(), Is.EqualTo("d1"));
            Assert.That(created.GetProperty("expires").GetBoolean(), Is.False);
        });

        var updated = await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "decisions",
            ["id"] = "d1",
            ["body"] = "They converge without locks.",
        });
        Assert.That(updated.GetProperty("created").GetBoolean(), Is.False,
            "A second remember at the same id merges rather than re-creating.");

        var recalled = await CallAsync(client, "repocontext_recall", new()
        {
            ["key"] = RepoContextKeys.Memory(RepoId, "decisions", "d1"),
        });
        Assert.Multiple(() =>
        {
            Assert.That(recalled.GetProperty("exists").GetBoolean(), Is.True);
            var fields = recalled.GetProperty("fields");
            Assert.That(fields.GetProperty("title").GetString(), Is.EqualTo("Adopt CRDTs"));
            Assert.That(fields.GetProperty("body").GetString(), Is.EqualTo("They converge without locks."));
            Assert.That(fields.GetProperty("kind").GetString(), Is.EqualTo("Decision"),
                "The immutable kind captured at creation survives the merge update.");
        });
    }

    [Test]
    public async Task Remember_generates_an_id_when_omitted()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var created = await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "notes",
            ["title"] = "no id supplied",
        });

        Assert.That(created.GetProperty("id").GetString(), Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task Remember_with_an_explicit_ttl_sets_an_expiry()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var created = await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "ephemeral",
            ["id"] = "e1",
            ["title"] = "short-lived",
            ["ttlSeconds"] = 3600L,
        });

        Assert.Multiple(() =>
        {
            Assert.That(created.GetProperty("expires").GetBoolean(), Is.True);
            Assert.That(created.GetProperty("expiresAtTicks").GetInt64(), Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task Remember_rejects_a_non_positive_ttl()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync("repocontext_remember", new Dictionary<string, object?>
        {
            ["repoId"] = RepoId,
            ["topic"] = "ephemeral",
            ["id"] = "bad",
            ["ttlSeconds"] = 0L,
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True, "A non-positive TTL is a caller-input error.");
    }

    // -- Update (CRDT merge) ---------------------------------------------------

    [Test]
    public async Task Update_patches_scalar_fields_and_tags_through_merge()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var key = RepoContextKeys.Memory(RepoId, "notes", "u1");

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "notes",
            ["id"] = "u1",
            ["title"] = "original",
        });

        var updated = await CallAsync(client, "repocontext_update", new()
        {
            ["key"] = key,
            ["fields"] = new Dictionary<string, string> { ["body"] = "patched body" },
            ["addTags"] = new[] { "reviewed" },
        });

        Assert.Multiple(() =>
        {
            Assert.That(updated.GetProperty("fieldsUpdated").GetInt32(), Is.EqualTo(1));
            Assert.That(updated.GetProperty("tagsAdded").GetInt32(), Is.EqualTo(1));
        });

        var recalled = await CallAsync(client, "repocontext_recall", new() { ["key"] = key });
        var fields = recalled.GetProperty("fields");
        Assert.Multiple(() =>
        {
            Assert.That(fields.GetProperty("title").GetString(), Is.EqualTo("original"),
                "The untouched title survives a field-level patch.");
            Assert.That(fields.GetProperty("body").GetString(), Is.EqualTo("patched body"));
            var tags = recalled.GetProperty("tags").EnumerateArray().Select(t => t.GetString()).ToArray();
            Assert.That(tags, Does.Contain("reviewed"));
        });
    }

    [Test]
    public async Task Update_of_a_missing_record_is_an_error()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync("repocontext_update", new Dictionary<string, object?>
        {
            ["key"] = RepoContextKeys.Memory(RepoId, "notes", "does-not-exist"),
            ["fields"] = new Dictionary<string, string> { ["body"] = "x" },
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True);
    }

    // -- Knowledge linking (typed edges + neighbor walk) -----------------------

    [Test]
    public async Task Remember_writes_links_that_recall_projects()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var tree = RepoContextKeys.Memory(RepoId, "glossary", "tree");
        var wal = RepoContextKeys.Memory(RepoId, "glossary", "wal");

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "glossary",
            ["id"] = "wal",
            ["title"] = "Write-ahead log",
        });

        var linked = await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "glossary",
            ["id"] = "tree",
            ["title"] = "B+ tree",
            ["addLinks"] = new Dictionary<string, string[]> { ["related"] = new[] { wal } },
        });
        Assert.That(linked.GetProperty("linksAdded").GetInt32(), Is.EqualTo(1));

        var recalled = await CallAsync(client, "repocontext_recall", new() { ["key"] = tree });
        var related = recalled.GetProperty("links").GetProperty("related")
            .EnumerateArray().Select(t => t.GetString()).ToArray();
        Assert.That(related, Is.EqualTo(new[] { wal }));
    }

    [Test]
    public async Task Update_rejects_a_malformed_link_target()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var key = RepoContextKeys.Memory(RepoId, "glossary", "shard");

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "glossary",
            ["id"] = "shard",
            ["title"] = "Shard",
        });

        var result = await client.CallToolAsync("repocontext_update", new Dictionary<string, object?>
        {
            ["key"] = key,
            ["addLinks"] = new Dictionary<string, string[]> { ["broader"] = new[] { "not a valid key" } },
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True, "A malformed link target is a caller-input error.");
    }

    [Test]
    public async Task Neighbors_walks_typed_edges_from_a_seed()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var root = RepoContextKeys.Memory(RepoId, "concepts", "lattice");
        var crdt = RepoContextKeys.Memory(RepoId, "concepts", "crdt");
        var wal = RepoContextKeys.Memory(RepoId, "concepts", "wal");

        foreach (var (id, title) in new[] { ("crdt", "CRDT"), ("wal", "WAL") })
        {
            await CallAsync(client, "repocontext_remember", new()
            {
                ["repoId"] = RepoId,
                ["topic"] = "concepts",
                ["id"] = id,
                ["title"] = title,
            });
        }

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "concepts",
            ["id"] = "lattice",
            ["title"] = "Orleans.Lattice",
            ["addLinks"] = new Dictionary<string, string[]> { ["narrower"] = new[] { crdt, wal } },
        });

        var walk = await CallAsync(client, "repocontext_neighbors", new()
        {
            ["key"] = root,
            ["relation"] = "narrower",
        });

        Assert.Multiple(() =>
        {
            Assert.That(walk.GetProperty("exists").GetBoolean(), Is.True);
            Assert.That(walk.GetProperty("truncated").GetBoolean(), Is.False);
            var reached = walk.GetProperty("neighbors").EnumerateArray()
                .Select(n => n.GetProperty("key").GetString())
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToArray();
            Assert.That(reached, Is.EqualTo(new[] { crdt, wal }.OrderBy(k => k, StringComparer.Ordinal).ToArray()));
        });
    }

    [Test]
    public async Task Neighbors_reports_a_missing_seed_as_absent()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var walk = await CallAsync(client, "repocontext_neighbors", new()
        {
            ["key"] = RepoContextKeys.Memory(RepoId, "concepts", "absent"),
        });

        Assert.Multiple(() =>
        {
            Assert.That(walk.GetProperty("exists").GetBoolean(), Is.False);
            Assert.That(walk.GetProperty("neighbors").GetArrayLength(), Is.EqualTo(0));
        });
    }

    // -- Forget (hard delete + soft lapse) -------------------------------------

    [Test]
    public async Task Forget_hard_deletes_an_entry()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var key = RepoContextKeys.Memory(RepoId, "notes", "del");

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "notes",
            ["id"] = "del",
            ["title"] = "delete me",
        });

        var forgotten = await CallAsync(client, "repocontext_forget", new() { ["key"] = key });
        Assert.Multiple(() =>
        {
            Assert.That(forgotten.GetProperty("mode").GetString(), Is.EqualTo("delete"));
            Assert.That(forgotten.GetProperty("existed").GetBoolean(), Is.True);
        });

        var recalled = await CallAsync(client, "repocontext_recall", new() { ["key"] = key });
        Assert.That(recalled.GetProperty("exists").GetBoolean(), Is.False,
            "A hard delete removes the entry immediately.");
    }

    [Test]
    public async Task Forget_soft_lapses_an_entry_with_a_short_ttl()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);
        var key = RepoContextKeys.Memory(RepoId, "notes", "lapse");

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId,
            ["topic"] = "notes",
            ["id"] = "lapse",
            ["title"] = "lapse me",
        });

        var lapsed = await CallAsync(client, "repocontext_forget", new()
        {
            ["key"] = key,
            ["lapse"] = true,
            ["lapseSeconds"] = 3600L,
        });
        Assert.Multiple(() =>
        {
            Assert.That(lapsed.GetProperty("mode").GetString(), Is.EqualTo("lapse"));
            Assert.That(lapsed.GetProperty("existed").GetBoolean(), Is.True);
            Assert.That(lapsed.GetProperty("expiresAtTicks").GetInt64(), Is.GreaterThan(0));
        });

        // The entry is still live immediately after the lapse, but now carries an expiry.
        var recalled = await CallAsync(client, "repocontext_recall", new() { ["key"] = key });
        Assert.Multiple(() =>
        {
            Assert.That(recalled.GetProperty("exists").GetBoolean(), Is.True,
                "A soft lapse leaves the entry readable until its short TTL elapses.");
            Assert.That(recalled.GetProperty("expires").GetBoolean(), Is.True);
        });
    }

    // -- Scan (ordered, paged) and list_topics ---------------------------------

    [Test]
    public async Task Scan_pages_a_memory_topic_in_key_order_with_a_continuation_token()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        for (var i = 0; i < 5; i++)
        {
            await CallAsync(client, "repocontext_remember", new()
            {
                ["repoId"] = RepoId,
                ["topic"] = "scan",
                ["id"] = $"s{i:D2}",
                ["title"] = $"entry {i}",
            });
        }

        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await CallAsync(client, "repocontext_scan", new()
            {
                ["repoId"] = RepoId,
                ["scope"] = "MemoryTopic",
                ["topic"] = "scan",
                ["pageSize"] = 2,
                ["continuationToken"] = token,
            });

            foreach (var entry in page.GetProperty("entries").EnumerateArray())
            {
                seen.Add(entry.GetProperty("id").GetString()!);
            }

            token = page.TryGetProperty("continuationToken", out var t) && t.ValueKind == JsonValueKind.String
                ? t.GetString()
                : null;
            pages++;
        }
        while (token is not null && pages < 10);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new[] { "s00", "s01", "s02", "s03", "s04" }),
                "The scan returns every live entry once, in ascending key order.");
            Assert.That(pages, Is.GreaterThan(1), "A pageSize of 2 over 5 entries spans multiple pages.");
        });
    }

    [Test]
    public async Task Scan_rejects_a_path_prefix_on_a_non_files_scope()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync("repocontext_scan", new Dictionary<string, object?>
        {
            ["repoId"] = RepoId,
            ["scope"] = "Memory",
            ["pathPrefix"] = "src/",
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True);
    }

    [Test]
    public async Task Scan_rejects_an_unknown_scope()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync("repocontext_scan", new Dictionary<string, object?>
        {
            ["repoId"] = RepoId,
            ["scope"] = "Nonsense",
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True);
    }

    [Test]
    public async Task List_topics_enumerates_distinct_topics_with_counts()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Writer, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId, ["topic"] = "alpha", ["id"] = "a1", ["title"] = "a1",
        });
        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId, ["topic"] = "alpha", ["id"] = "a2", ["title"] = "a2",
        });
        await CallAsync(client, "repocontext_remember", new()
        {
            ["repoId"] = RepoId, ["topic"] = "beta", ["id"] = "b1", ["title"] = "b1",
        });

        var result = await CallAsync(client, "repocontext_list_topics", new() { ["repoId"] = RepoId });

        var topics = result.GetProperty("topics").EnumerateArray()
            .ToDictionary(t => t.GetProperty("topic").GetString()!, t => t.GetProperty("entryCount").GetInt32());

        Assert.Multiple(() =>
        {
            Assert.That(topics["alpha"], Is.EqualTo(2));
            Assert.That(topics["beta"], Is.EqualTo(1));
        });
    }

    // -- Recall of a structural record -----------------------------------------

    [Test]
    public async Task Recall_projects_a_seeded_structural_file_record()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = "src/a.cs",
            Language = RepoContextValues.Lww("csharp", new HybridLogicalClock { WallClockTicks = 100 }),
        };
        await tree.SetAsync(RepoContextKeys.File(RepoId, "src/a.cs"), serializer.SerializeToArray(node), Ct);

        var recalled = await CallAsync(client, "repocontext_recall", new()
        {
            ["key"] = RepoContextKeys.File(RepoId, "src/a.cs"),
        });

        Assert.Multiple(() =>
        {
            Assert.That(recalled.GetProperty("exists").GetBoolean(), Is.True);
            Assert.That(recalled.GetProperty("kind").GetString(), Is.EqualTo("File"));
            Assert.That(recalled.GetProperty("fields").GetProperty("language").GetString(), Is.EqualTo("csharp"));
        });
    }

    [Test]
    public async Task Recall_of_an_absent_key_reports_not_exists()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var recalled = await CallAsync(client, "repocontext_recall", new()
        {
            ["key"] = RepoContextKeys.Memory(RepoId, "notes", "never-written"),
        });

        Assert.That(recalled.GetProperty("exists").GetBoolean(), Is.False);
    }

    [Test]
    public async Task Recall_of_a_malformed_key_is_an_error()
    {
        await using var harness = await StartAsync(RepoContextMcpAuthPosture.Reader, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync("repocontext_recall", new Dictionary<string, object?>
        {
            ["key"] = "not-a-valid-key",
        }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True);
    }
}
