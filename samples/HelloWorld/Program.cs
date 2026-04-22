using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// Host a single-silo Orleans cluster in-process and register Lattice with
// in-memory grain storage + reminders. This is the minimum viable setup for
// exercising the tree locally.
using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));
    })
    .Build();

Console.Write("Silo Starting...");
await host.StartAsync();
Console.WriteLine(" ready.");

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>("hello-world");

Console.WriteLine("Orleans.Lattice HelloWorld sample");
Console.WriteLine("Commands: create, read, update, delete, list, exit");
Console.WriteLine();

while (true)
{
    Console.Write("Next Operation: ");
    Console.Out.Flush();
    var command = (Console.ReadLine() ?? string.Empty).Trim().ToLowerInvariant();

    if (command is "exit" or "quit")
    {
        break;
    }

    var sw = Stopwatch.StartNew();
    try
    {
        switch (command)
        {
            case "create":
            case "update":
            {
                var key = Prompt("key");
                var value = Prompt("value");
                sw.Restart();
                await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
                sw.Stop();
                Report(success: true, $"{command} '{key}' = '{value}'", sw.Elapsed);
                break;
            }
            case "read":
            {
                var key = Prompt("key");
                sw.Restart();
                var bytes = await tree.GetAsync(key);
                sw.Stop();
                if (bytes is null)
                {
                    Report(success: false, $"read '{key}' -> <not found>", sw.Elapsed);
                }
                else
                {
                    Report(success: true, $"read '{key}' = '{Encoding.UTF8.GetString(bytes)}'", sw.Elapsed);
                }
                break;
            }
            case "delete":
            {
                var key = Prompt("key");
                sw.Restart();
                var existed = await tree.DeleteAsync(key);
                sw.Stop();
                Report(success: existed, existed ? $"delete '{key}'" : $"delete '{key}' -> <not found>", sw.Elapsed);
                break;
            }
            case "list":
            {
                sw.Restart();
                var count = 0;
                await foreach (var entry in tree.ScanEntriesAsync())
                {
                    Console.WriteLine($"  {entry.Key} = {Encoding.UTF8.GetString(entry.Value)}");
                    count++;
                }
                sw.Stop();
                Report(success: true, $"list -> {count} entr{(count == 1 ? "y" : "ies")}", sw.Elapsed);
                break;
            }
            case "":
                continue;
            default:
                Console.WriteLine($"Unknown command '{command}'. Try: create, read, update, delete, list, exit.");
                continue;
        }
    }
    catch (Exception ex)
    {
        sw.Stop();
        Report(success: false, $"{command} failed: {ex.GetType().Name}: {ex.Message}", sw.Elapsed);
    }
}

await host.StopAsync();

static string Prompt(string label)
{
    Console.Write($"  {label}: ");
    return Console.ReadLine() ?? string.Empty;
}

static void Report(bool success, string message, TimeSpan elapsed)
{
    var previous = Console.ForegroundColor;
    Console.ForegroundColor = success ? ConsoleColor.Green : ConsoleColor.Red;
    Console.WriteLine($"  [{(success ? "OK" : "FAIL")}] {message} ({elapsed.TotalMilliseconds:F2} ms)");
    Console.ForegroundColor = previous;
}
