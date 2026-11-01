using Microsoft.Playwright;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The bounded-memory proof for the streaming import (#5365): on the out-of-process estate, a
/// 200 MB NDJSON file is imported through the Explorer's own Import dialog, once as a seed and
/// once as an upsert, while the memory of the Explorer head and of every west silo is watched.
/// No process's live managed heap may rise more than <see cref="LiveHeapCeilingBytes"/> above
/// its baseline at any point, which no process could manage if any hop - the browser upload,
/// the Explorer's reader and channel, the gateway, or a silo's streaming handler - held the
/// file, or any fixed fraction of it above a sixth.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is measured (#5475).</b> The live heap is read through each host's memory probe,
/// straight after a forced full compacting collection: once settled (an aggressive collection
/// that also returns free memory to the operating system) before the import, about once a
/// second while it runs, and settled again after it. It is what the process references at
/// that instant, so it does not depend on when the garbage collector last ran. The working
/// set is not the gate: under the server collector it also carries the allocation budgets the
/// collector keeps committed, which its dynamic adaptation resizes with load, and which it
/// returns to the operating system only when it chooses. The first version of this proof
/// gated on working-set growth above a baseline read five seconds after the warm-up, and the
/// readings CI recorded showed what that measured: whether the collector had returned memory
/// in those five seconds. A silo whose working set the warm-up took to 498.6 MB had dropped
/// to a 399.9 MB baseline and then peaked at 516.4 MB, and was charged with 116.5 MB.
/// </para>
/// <para>
/// The working set is still bounded, as a sanity bound: its peak may not rise more than
/// <see cref="WorkingSetCeilingBytes"/> - half the file - above the larger of its baseline and
/// the peak the same process reached during the warm-up's quarter-size file. A process holding
/// the file would add at least the 150 MB the two files differ by; over fourteen CI readings
/// before #5475 the largest rise of a silo above its warm-up peak was 72 MB, the collector's
/// budgets rather than the file.
/// </para>
/// <para>
/// Each import is preceded by a warm-up import of a quarter of the size through the same
/// path. The warm-up brings the code JIT-compiled and each process's activations, pools and
/// collector budgets to their working size, so what is measured is what the file's size adds,
/// not what any first import costs.
/// </para>
/// <para>
/// The files are shaped so that a silo's growth measures what it streamed rather than what it
/// stored. The seed file's 32 KB lines carry padding the import reads and discards (the value
/// pointer stores one small field), so a 200 MB seed builds a tree of 6,400 small keys. A silo
/// never sees the seed's file, only its entries; the hop the seed proves is the browser upload
/// and the Explorer's reader, which read the whole file twice. The
/// upsert file's lines are whole records written to a bounded set of keys, so the full 200 MB
/// streams through the gateway and both silos while the tree keeps only the latest record per
/// key.
/// </para>
/// </remarks>
[TestFixture]
[Journey(JourneyId)]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class ImportMemoryTests
{
    /// <summary>
    /// How far any measured process's live heap may rise above its baseline during an import.
    /// The import's own budget is far smaller - the Explorer's channel of 1,024 entries, and a
    /// batch of 256 upsert records (1 MB) or a chunk of 1,000 seed entries in flight per hop -
    /// so the ceiling leaves room for a few batches queued on each hop and for the stored tree,
    /// while staying a sixth of the file.
    /// </summary>
    public const long LiveHeapCeilingBytes = ImportMemoryBudget.LiveHeapCeilingBytes;

    /// <summary>How far any measured process's working set may rise above the larger of its baseline and its warm-up peak: half the file.</summary>
    public const long WorkingSetCeilingBytes = FileBytes / 2;

    private const string JourneyId = "g1-import-memory";
    private const long FileBytes = 200L * 1024 * 1024;
    private const long WarmUpBytes = FileBytes / 4;
    private const int UpsertKeys = 512;
    private const string SeedWarmUpTree = "memory-seed-warmup";
    private const string SeedTree = "memory-seed";
    private const string UpsertTree = "memory-upsert";

    private static readonly TimeSpan ImportTimeout = TimeSpan.FromMinutes(12);

    private JourneyEstate _estate = null!;
    private string _files = null!;

    /// <summary>Boots the Contoso west region and creates the trees the imports write.</summary>
    [OneTimeSetUp]
    public async Task StartEstateAsync()
    {
        // No line feed: nothing but the imports writes to the silos while they are measured.
        _estate = await JourneyEstate.StartAsync(new JourneyEstateOptions { LineFeed = false });
        _files = Path.Combine(Path.GetTempPath(), "lattice-import-memory-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_files);

        var admin = LatticeTreeAdminApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization);
        foreach (var tree in new[] { SeedWarmUpTree, SeedTree, UpsertTree })
        {
            await admin.CreateTreeAsync(tree, shardCount: 4);
        }
    }

    /// <summary>Deletes the generated files and stops the estate.</summary>
    [OneTimeTearDown]
    public async Task StopEstateAsync()
    {
        try
        {
            if (_files is not null && Directory.Exists(_files))
            {
                Directory.Delete(_files, recursive: true);
            }
        }
        finally
        {
            if (_estate is not null)
            {
                await _estate.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// A 200 MB NDJSON file seeds an empty tree through the Import dialog: both of the seed's
    /// passes stream the file from the browser, and no process grows past the ceiling.
    /// </summary>
    [Test]
    [JourneyStep("seed", "Olivia seeds an empty tree from a 200 MB NDJSON file")]
    public async Task A_200_MB_seed_keeps_the_explorer_and_every_silo_under_the_ceiling()
    {
        var warmUp = Path.Combine(_files, "warm-up-seed.ndjson");
        var warmUpLines = ImportFiles.WriteSeed(warmUp, WarmUpBytes);
        var file = Path.Combine(_files, "orders-200mb.ndjson");
        var lines = ImportFiles.WriteSeed(file, FileBytes);

        await using var olivia = await _estate.SignInAsAsync(Personas.Olivia);

        var dialog = await OpenSeedAsync(olivia, SeedWarmUpTree);
        await ChooseAsync(dialog, warmUp, "bulk/{/id}", "/n");
        var warmUpReadings = await WarmUpAsync(dialog, "Seed tree", warmUpLines);

        dialog = await OpenSeedAsync(olivia, SeedTree);
        await ChooseAsync(dialog, file, "bulk/{/id}", "/n");
        await olivia.SubstepAsync("file-chosen", "A 200 MB NDJSON file is chosen; the key comes from /id and the value from /n");

        var growth = await MeasureAsync(olivia, dialog, "Seed tree", lines, "seed-progress", "The seed streams the file: the pre-flight pass checks its order before anything is written");
        await olivia.SubstepAsync("succeeded", $"Seeded {ImportFiles.Count(lines)} entries; no process's live heap grew by 32 MB");

        growth = WithWarmUp(growth, warmUpReadings);
        var (found, missing) = await FindEveryKeyAsync(SeedTree, Enumerable.Range(0, lines).Select(line => $"bulk/{line:D8}"));
        var scanned = await CountKeysAsync(SeedTree);
        var stats = await LatticeTreeAdminApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization).GetTreeStatsAsync(SeedTree);
        Report("Seed", growth,
            $"A point read of every one of the file's {ImportFiles.Count(lines)} keys finds {ImportFiles.Count(found)}"
            + (missing.Count > 0 ? $" (missing, for example: {string.Join(", ", missing)})" : string.Empty)
            + $". A paged range scan of the tree reads {ImportFiles.Count(scanned)} keys, and its tree stats report {ImportFiles.Count(stats.TotalLiveKeys)} live keys.");

        // Issue #5460: the tree a seed builds is exact through every read path, not only point
        // reads - the header's count and a scan that reaches the file's last key.
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
        var header = olivia.Shell.Content.GetByText(new System.Text.RegularExpressions.Regex(@"^[\d,]+ live keys?$"));
        await Expect(header).ToBeVisibleAsync(new() { Timeout = 30_000 });
        await olivia.SubstepAsync("tree-header", $"The tree header counts the seeded tree's live keys: {ImportFiles.Count(lines)} are expected");
        var headerText = await header.InnerTextAsync();

        var lastKey = $"bulk/{lines - 1:D8}";
        await olivia.Shell.GotoAsync($"/data/{SeedTree}?prefix={Uri.EscapeDataString(lastKey[..^1])}");
        var lastRow = olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = lastKey, Exact = true });
        await Expect(lastRow).ToBeVisibleAsync(new() { Timeout = 30_000 });
        await olivia.SubstepAsync("keys-tail", $"A prefix scan of the Keys panel reaches the file's last key, {lastKey}");

        Assert.Multiple(() =>
        {
            AssertUnderCeiling("seed", growth);
            Assert.That(found, Is.EqualTo(lines), "every line of the file is a key of the tree");
            Assert.That(scanned, Is.EqualTo(lines), "a paged range scan reads every key once");
            Assert.That(stats.TotalLiveKeys, Is.EqualTo(lines), "the tree statistics count every key once");
            Assert.That(headerText, Is.EqualTo($"{ImportFiles.Count(lines)} live keys"), "the tree header counts every key once");
        });
    }

    /// <summary>
    /// A 200 MB NDJSON file of whole records is upserted into a tree through the Import dialog:
    /// the full file streams through the gateway and the silos, and no process grows past the
    /// ceiling.
    /// </summary>
    [Test]
    [JourneyStep("upsert", "Olivia upserts a 200 MB NDJSON file of whole records")]
    public async Task A_200_MB_upsert_keeps_the_explorer_and_every_silo_under_the_ceiling()
    {
        var warmUp = Path.Combine(_files, "warm-up-upsert.ndjson");
        var warmUpLines = ImportFiles.WriteUpsert(warmUp, WarmUpBytes, UpsertKeys);
        var file = Path.Combine(_files, "order-updates-200mb.ndjson");
        var lines = ImportFiles.WriteUpsert(file, FileBytes, UpsertKeys);

        await using var olivia = await _estate.SignInAsAsync(Personas.Olivia);

        var dialog = await OpenUpsertAsync(olivia, UpsertTree);
        await ChooseAsync(dialog, warmUp, "order/{/id}", valuePointer: null);
        var warmUpReadings = await WarmUpAsync(dialog, "Import", warmUpLines);

        dialog = await OpenUpsertAsync(olivia, UpsertTree);
        await ChooseAsync(dialog, file, "order/{/id}", valuePointer: null);
        await olivia.SubstepAsync("file-chosen", "A 200 MB NDJSON file of whole order records is chosen for an upsert");

        var growth = await MeasureAsync(olivia, dialog, "Import", lines, "upsert-progress", "The upsert streams every record through the gateway to the silos, a batch at a time");
        await olivia.SubstepAsync("succeeded", $"Wrote {ImportFiles.Count(lines)} records; no process's live heap grew by 32 MB");

        growth = WithWarmUp(growth, warmUpReadings);
        var (found, _) = await FindEveryKeyAsync(UpsertTree, Enumerable.Range(0, UpsertKeys).Select(key => $"order/{key:D6}"));
        var scanned = await CountKeysAsync(UpsertTree);
        Report("Upsert", growth, $"A point read of each of the {UpsertKeys} keys finds {found}; a range scan of the tree reads {ImportFiles.Count(scanned)} keys.");
        Assert.Multiple(() =>
        {
            AssertUnderCeiling("upsert", growth);
            Assert.That(found, Is.EqualTo(UpsertKeys), "every record landed on its key");
            Assert.That(scanned, Is.EqualTo(UpsertKeys), "the records land on their keys and nothing else");
        });
    }

    private static async Task<ILocator> OpenSeedAsync(PersonaSession session, string tree)
    {
        await session.Shell.GotoAsync($"/data/{tree}?import=seed");
        var dialog = session.Page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}" });
        await Expect(dialog).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(dialog.GetByLabel("Mode")).ToHaveValueAsync("Seed");
        return dialog;
    }

    private static async Task<ILocator> OpenUpsertAsync(PersonaSession session, string tree)
    {
        await session.Shell.GotoAsync($"/data/{tree}");
        var import = session.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Import...", Exact = true });
        await Expect(import).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await import.ClickAsync();
        var dialog = session.Page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}" });
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(dialog.GetByLabel("Mode")).ToHaveValueAsync("Upsert");
        return dialog;
    }

    private static async Task ChooseAsync(ILocator dialog, string file, string keyTemplate, string? valuePointer)
    {
        await dialog.Locator("#data-import-file").SetInputFilesAsync(file);
        await Expect(dialog.GetByLabel("Format")).ToHaveValueAsync("Ndjson");
        await dialog.GetByLabel("Key", new() { Exact = true }).FillAsync(keyTemplate);
        if (valuePointer is not null)
        {
            await dialog.GetByLabel("Value", new() { Exact = true }).SelectOptionAsync("Field");
            await dialog.GetByLabel("Value pointer").FillAsync(valuePointer);
        }
    }

    /// <summary>Runs the warm-up import, returning what each process's memory did during it.</summary>
    private async Task<IReadOnlyList<WorkingSetGrowth>> WarmUpAsync(ILocator dialog, string start, long lines)
    {
        await using var sampler = await StartSamplerAsync();
        await dialog.GetByRole(AriaRole.Button, new() { Name = start, Exact = true }).ClickAsync();
        await ExpectSucceededAsync(dialog, lines);
        var readings = await sampler.StopAsync();
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
        return readings;
    }

    /// <summary>Adds the warm-up's working-set peak and live-heap growth to each process's reading.</summary>
    private static List<WorkingSetGrowth> WithWarmUp(IReadOnlyList<WorkingSetGrowth> growth, IReadOnlyList<WorkingSetGrowth> warmUp) =>
        [.. growth.Select((reading, i) => reading with
        {
            WarmUpPeak = warmUp[i].Peak,
            WarmUpLiveGrowth = warmUp[i].LiveGrowth,
            WarmUpLiveBaseline = warmUp[i].LiveBaseline,
        })];

    private async Task<WorkingSetSampler> StartSamplerAsync() =>
        await WorkingSetSampler.StartAsync(MeasuredProcesses(), (process, settle) => _estate.ReadMemoryAsync(process.ProbeRoot, settle));

    /// <summary>Reads every one of <paramref name="keys"/> by a point read, returning how many exist and a few that do not.</summary>
    private async Task<(int Found, IReadOnlyList<string> Missing)> FindEveryKeyAsync(string tree, IEnumerable<string> keys)
    {
        var data = LatticeDataApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization);
        var found = 0;
        var missing = new System.Collections.Concurrent.ConcurrentQueue<string>();
        await Parallel.ForEachAsync(keys, new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (key, cancellationToken) =>
        {
            var read = await data.GetAsync(new DataGetRequest { TreeId = tree, Key = key }, cancellationToken);
            if (read.Found)
            {
                Interlocked.Increment(ref found);
            }
            else
            {
                missing.Enqueue(key);
            }
        });

        return (found, [.. missing.Order(StringComparer.Ordinal).Take(5)]);
    }

    private static async Task ExpectSucceededAsync(ILocator dialog, long lines)
    {
        await Expect(dialog.Locator(".lt-data-import")).ToHaveAttributeAsync(
            "data-lt-import-state", "succeeded", new() { Timeout = (float)ImportTimeout.TotalMilliseconds });
        await Expect(dialog.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync(
            "data-lt-import-committed", lines.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Expect(dialog.Locator("[data-lt-import-refused]")).ToHaveAttributeAsync("data-lt-import-refused", "0");
    }

    /// <summary>Settles and baselines each process, runs the import, and returns each process's readings.</summary>
    private async Task<IReadOnlyList<WorkingSetGrowth>> MeasureAsync(
        PersonaSession session, ILocator dialog, string start, long lines, string progressSubstep, string progressTitle)
    {
        await using var sampler = await StartSamplerAsync();

        await dialog.GetByRole(AriaRole.Button, new() { Name = start, Exact = true }).ClickAsync();
        await Expect(dialog.GetByRole(AriaRole.Progressbar)).ToBeVisibleAsync();
        await session.SubstepAsync(progressSubstep, progressTitle);
        await ExpectSucceededAsync(dialog, lines);

        // Stopping settles each process once more: whatever the import left behind is in the
        // live heap that settled collection reads.
        return await sampler.StopAsync();
    }

    private List<MeasuredProcess> MeasuredProcesses()
    {
        var region = _estate.WestRegion;
        var explorer = region.ExplorerProcesses[0];
        var processes = new List<MeasuredProcess>
        {
            new(explorer.Name, explorer.ProcessId ?? throw new InvalidOperationException("The Explorer head is not running."),
                new Uri(region.ExplorerUri.GetLeftPart(UriPartial.Authority) + "/")),
        };
        foreach (var silo in region.Silos)
        {
            processes.Add(new(silo.Process.Name, silo.Process.ProcessId ?? throw new InvalidOperationException($"{silo.Process.Name} is not running."),
                new Uri($"http://127.0.0.1:{silo.HttpPort}/")));
        }

        return processes;
    }

    /// <summary>Counts the keys a paged range read through the Data API returns, for the report.</summary>
    private async Task<long> CountKeysAsync(string tree)
    {
        var data = LatticeDataApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization);
        long count = 0;
        string? token = null;
        do
        {
            var page = await data.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 1000, ContinuationToken = token });
            count += page.Entries.Count;

            token = page.ContinuationToken;
        }
        while (token is not null);

        return count;
    }

    private void Report(string mode, IReadOnlyList<WorkingSetGrowth> growth, string note)
    {
        var table = WorkingSetGrowth.ToMarkdown($"{mode}: memory of each process", FileBytes, growth) + Environment.NewLine + note + Environment.NewLine;
        TestContext.Out.WriteLine(table);
        JobSummary.Append(table);
        File.AppendAllText(Path.Combine(_estate.ArtifactsDirectory, "import-memory.md"), table + Environment.NewLine);
    }

    private static void AssertUnderCeiling(string mode, IReadOnlyList<WorkingSetGrowth> growth)
    {
        foreach (var reading in growth)
        {
            Assert.That(reading.Samples, Is.GreaterThan(4), $"{reading.Process}'s working set was sampled while the import ran");
            Assert.That(reading.LiveSamples, Is.GreaterThan(2), $"{reading.Process}'s live heap was read while the import ran");
            Assert.That(ImportMemoryBudget.Violations(reading, mode, FileBytes), Is.Empty,
                $"{reading.Process}'s live heap stays within the import's memory budget during a {mode} of a {WorkingSetGrowth.Mb(FileBytes)} file");
            Assert.That(reading.Growth, Is.LessThan(WorkingSetCeilingBytes),
                $"{reading.Process}'s working set rose {WorkingSetGrowth.Mb(reading.Growth)} above the larger of its {WorkingSetGrowth.Mb(reading.Baseline)} baseline and its {WorkingSetGrowth.Mb(reading.WarmUpPeak ?? 0)} warm-up peak during a {mode} of a {WorkingSetGrowth.Mb(FileBytes)} file");
        }
    }
}
