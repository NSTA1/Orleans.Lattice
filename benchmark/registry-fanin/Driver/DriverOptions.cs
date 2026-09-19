using System.Globalization;

namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// The parsed driver command line.
/// </summary>
internal sealed class DriverOptions
{
    /// <summary>The verb to run.</summary>
    public string Verb { get; private set; } = "help";

    /// <summary>How many trees the fleet spans.</summary>
    public int Trees { get; private set; } = 20;

    /// <summary>
    /// How many of the fleet's trees carry synthetic load - the 'heavily loaded
    /// other trees' condition. Defaults to all of them.
    /// </summary>
    public int LoadTrees { get; private set; }

    /// <summary>Target operations per second across the whole load.</summary>
    public double Rate { get; private set; } = 50;

    /// <summary>How long to sustain the load.</summary>
    public TimeSpan Duration { get; private set; } = TimeSpan.FromMinutes(1);

    /// <summary>The fraction of load operations that are writes.</summary>
    public double WriteFraction { get; private set; } = 0.5;

    /// <summary>The tree-id prefix the driver owns.</summary>
    public string Prefix { get; private set; } = "fanin_";

    /// <summary>The silo gateway port to join through.</summary>
    public int GatewayPort { get; private set; } = 30000;

    /// <summary>The Orleans cluster id.</summary>
    public string ClusterId { get; private set; } = "dev";

    /// <summary>The Orleans service id.</summary>
    public string ServiceId { get; private set; } = "dev";

    /// <summary>The client response deadline.</summary>
    public TimeSpan ResponseTimeout { get; private set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to keep retrying the initial cluster join.</summary>
    public TimeSpan ConnectTimeout { get; private set; } = TimeSpan.FromMinutes(5);

    /// <summary>How many fleet operations to run at once during create/teardown.</summary>
    public int Parallelism { get; private set; } = 8;

    /// <summary>An in-flight ceiling for the load driver, or zero for none.</summary>
    public int MaxInFlight { get; private set; }

    /// <summary>Where to write the JSON report.</summary>
    public string? OutputPath { get; private set; }

    /// <summary>
    /// Parses <paramref name="args"/>.
    /// </summary>
    /// <param name="args">The raw command line.</param>
    /// <returns>The parsed options.</returns>
    /// <exception cref="ArgumentException">A switch was unrecognised or malformed.</exception>
    public static DriverOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var options = new DriverOptions();
        if (args.Length > 0 && !args[0].StartsWith("--", StringComparison.Ordinal))
        {
            options.Verb = args[0];
        }

        for (var i = options.Verb == "help" ? 0 : 1; i < args.Length; i++)
        {
            var name = args[i];
            if (!name.StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Unexpected argument '{name}'.");
            }

            string Value()
            {
                if (i + 1 >= args.Length)
                {
                    throw new ArgumentException($"Switch '{name}' needs a value.");
                }

                return args[++i];
            }

            switch (name)
            {
                case "--trees": options.Trees = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--load-trees": options.LoadTrees = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--rate": options.Rate = double.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--duration": options.Duration = TimeSpan.FromSeconds(double.Parse(Value(), CultureInfo.InvariantCulture)); break;
                case "--write-fraction": options.WriteFraction = double.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--prefix": options.Prefix = Value(); break;
                case "--gateway-port": options.GatewayPort = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--cluster-id": options.ClusterId = Value(); break;
                case "--service-id": options.ServiceId = Value(); break;
                case "--response-timeout": options.ResponseTimeout = TimeSpan.FromSeconds(double.Parse(Value(), CultureInfo.InvariantCulture)); break;
                case "--connect-timeout": options.ConnectTimeout = TimeSpan.FromSeconds(double.Parse(Value(), CultureInfo.InvariantCulture)); break;
                case "--parallelism": options.Parallelism = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--max-in-flight": options.MaxInFlight = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--out": options.OutputPath = Value(); break;
                default: throw new ArgumentException($"Unrecognised switch '{name}'.");
            }
        }

        if (options.LoadTrees <= 0 || options.LoadTrees > options.Trees)
        {
            options.LoadTrees = options.Trees;
        }

        return options;
    }

    /// <summary>The usage text.</summary>
    public const string Usage = """
        Orleans.Lattice registry fan-in driver.

        Verbs:
          create    Register K trees and give each a first write, so each acquires
                    its per-tree background services.
          load      Drive synthetic read/write load against a subset of the fleet.
          probe     Drive registry point reads (Resolve / GetEntry / GetShardMap),
                    mirroring the member mix seen in the observed storm.
          teardown  DeleteTreeAsync then UnregisterAsync for every fleet tree.
          list      Report the registry's current tree ids.

        Switches:
          --trees N            fleet size K (default 20)
          --load-trees N       how many fleet trees carry load (default: all)
          --rate R             target operations/second (default 50)
          --duration S         seconds to sustain the load (default 60)
          --write-fraction F   fraction of load ops that are writes (default 0.5)
          --prefix P           tree-id prefix this driver owns (default fanin_)
          --gateway-port N     silo gateway port (default 30000)
          --cluster-id ID      Orleans cluster id (default dev)
          --service-id ID      Orleans service id (default dev)
          --response-timeout S client response deadline (default 30)
          --connect-timeout S  how long to retry the initial join (default 300)
          --parallelism N      concurrent create/teardown operations (default 8)
          --max-in-flight N    in-flight ceiling for load, 0 = none (default 0)
          --out PATH           write the JSON report here
        """;
}
