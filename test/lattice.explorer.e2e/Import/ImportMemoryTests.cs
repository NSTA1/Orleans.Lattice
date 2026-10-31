using Microsoft.Playwright;
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
/// once as an upsert, while the working set of the Explorer head and of every west silo is
/// sampled. Each must grow less than <see cref="CeilingBytes"/> above its pre-import baseline,
/// which no process could do if any hop - the browser upload, the Explorer's reader and
/// channel, the gateway, or a silo's streaming handler - held the file.
/// </summary>
/// <remarks>
/// <para>
/// Each import is preceded by a warm-up import of a tenth of the size through the same path,
/// and the baseline is taken after it. The warm-up brings the code JIT-compiled and each
/// process's server-GC heaps to their working size, so the measured growth is what the file's
/// size adds, not what any first import costs.
/// </para>
/// <para>
/// The files are shaped so that a silo's growth measures what it streamed rather than what it
/// stored. The seed file's lines carry padding the import reads and discards (the value
/// pointer stores one small field), so a 200 MB seed builds a tree of a few megabytes. The
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
    /// <summary>How far any measured process may grow above its baseline during an import.</summary>
    public const long CeilingBytes = 64L * 1024 * 1024;

    private const string JourneyId = "g1-import-memory";
    private const long FileBytes = 200L * 1024 * 1024;
    private const long WarmUpBytes = FileBytes / 10;
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
        _estate = await JourneyEstate.StartAsync();
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
        await RunAsync(dialog, "Seed tree", warmUpLines);

        dialog = await OpenSeedAsync(olivia, SeedTree);
        await ChooseAsync(dialog, file, "bulk/{/id}", "/n");
        await olivia.SubstepAsync("file-chosen", "A 200 MB NDJSON file is chosen; the key comes from /id and the value from /n");

        var growth = await MeasureAsync(olivia, dialog, "Seed tree", lines, "seed-progress", "The seed streams the file: the pre-flight pass checks its order before anything is written");
        await olivia.SubstepAsync("succeeded", $"Seeded {ImportFiles.Count(lines)} entries; no process grew by 64 MB");

        var stats = await LatticeTreeAdminApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization).GetTreeStatsAsync(SeedTree);
        Assert.That(stats.TotalLiveKeys, Is.EqualTo(lines), "every line of the file is a key of the tree");
        Report("Seed", growth);
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
        await RunAsync(dialog, "Import", warmUpLines);

        dialog = await OpenUpsertAsync(olivia, UpsertTree);
        await ChooseAsync(dialog, file, "order/{/id}", valuePointer: null);
        await olivia.SubstepAsync("file-chosen", "A 200 MB NDJSON file of whole order records is chosen for an upsert");

        var growth = await MeasureAsync(olivia, dialog, "Import", lines, "upsert-progress", "The upsert streams every record through the gateway to the silos, a batch at a time");
        await olivia.SubstepAsync("succeeded", $"Wrote {ImportFiles.Count(lines)} records; no process grew by 64 MB");
        Report("Upsert", growth);
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

    private static async Task RunAsync(ILocator dialog, string start, long lines)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = start, Exact = true }).ClickAsync();
        await ExpectSucceededAsync(dialog, lines);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
    }

    private static async Task ExpectSucceededAsync(ILocator dialog, long lines)
    {
        await Expect(dialog.Locator(".lt-data-import")).ToHaveAttributeAsync(
            "data-lt-import-state", "succeeded", new() { Timeout = (float)ImportTimeout.TotalMilliseconds });
        await Expect(dialog.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync(
            "data-lt-import-committed", lines.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Expect(dialog.Locator("[data-lt-import-refused]")).ToHaveAttributeAsync("data-lt-import-refused", "0");
    }

    /// <summary>Takes the baseline, runs the import, and returns each process's growth.</summary>
    private async Task<IReadOnlyList<WorkingSetGrowth>> MeasureAsync(
        PersonaSession session, ILocator dialog, string start, long lines, string progressSubstep, string progressTitle)
    {
        // Let the warm-up's work settle before the baseline is read.
        await Task.Delay(TimeSpan.FromSeconds(5));
        await using var sampler = await WorkingSetSampler.StartAsync(MeasuredProcesses());

        await dialog.GetByRole(AriaRole.Button, new() { Name = start, Exact = true }).ClickAsync();
        await Expect(dialog.GetByRole(AriaRole.Progressbar)).ToBeVisibleAsync();
        await session.SubstepAsync(progressSubstep, progressTitle);
        await ExpectSucceededAsync(dialog, lines);

        // A late burst (the last batch's acknowledgement, the commit) is still sampled.
        await Task.Delay(TimeSpan.FromSeconds(2));
        return await sampler.StopAsync();
    }

    private List<(string Name, int ProcessId)> MeasuredProcesses()
    {
        var region = _estate.WestRegion;
        var processes = new List<(string, int)>
        {
            (region.ExplorerProcesses[0].Name, region.ExplorerProcesses[0].ProcessId ?? throw new InvalidOperationException("The Explorer head is not running.")),
        };
        foreach (var silo in region.Silos)
        {
            processes.Add((silo.Process.Name, silo.Process.ProcessId ?? throw new InvalidOperationException($"{silo.Process.Name} is not running.")));
        }

        return processes;
    }

    private void Report(string mode, IReadOnlyList<WorkingSetGrowth> growth)
    {
        var table = WorkingSetGrowth.ToMarkdown($"{mode}: working set of each process", FileBytes, growth);
        TestContext.Out.WriteLine(table);
        JobSummary.Append(table);
        File.AppendAllText(Path.Combine(_estate.ArtifactsDirectory, "import-memory.md"), table + Environment.NewLine);

        Assert.Multiple(() =>
        {
            foreach (var reading in growth)
            {
                Assert.That(reading.Samples, Is.GreaterThan(4), $"{reading.Process} was sampled while the import ran");
                Assert.That(reading.Growth, Is.LessThan(CeilingBytes),
                    $"{reading.Process} grew {WorkingSetGrowth.Mb(reading.Growth)} above its {WorkingSetGrowth.Mb(reading.Baseline)} baseline during a {mode.ToLowerInvariant()} of a {WorkingSetGrowth.Mb(FileBytes)} file");
            }
        });
    }
}
