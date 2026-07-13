using System.Globalization;

namespace Orleans.Lattice.Samples.ClusterScaling.LoadDriver;

/// <summary>
/// Parsed command-line options for the ClusterScaling load driver. Kept as a
/// simple option bag with a hand-rolled parser so the console has no argument
/// dependency; <c>drive-load.ps1</c> passes these through.
/// </summary>
internal sealed class LoadDriverOptions
{
    /// <summary>The data-API gRPC address (an <c>https://...</c> URL against the ACA ingress).</summary>
    public required string Address { get; init; }

    /// <summary>The admin username presented in the Basic credential.</summary>
    public required string Username { get; init; }

    /// <summary>The admin password presented in the Basic credential.</summary>
    public required string Password { get; init; }

    /// <summary>Offered operations per second (the intended load, independent of cluster capacity).</summary>
    public required double OfferedRatePerSecond { get; init; }

    /// <summary>How long to sustain the offered load.</summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>The number of distinct trees load is spread across (activation fan-out).</summary>
    public required int TreeCount { get; init; }

    /// <summary>The number of distinct keys per tree cycle (leaf-grain activation fan-out).</summary>
    public required long KeySpace { get; init; }

    /// <summary>Fraction of operations issued as reads (the remainder are writes).</summary>
    public required double ReadRatio { get; init; }

    /// <summary>The fixed write payload size in bytes. Small on purpose to keep the storage axis flat.</summary>
    public required int PayloadBytes { get; init; }

    /// <summary>The maximum number of concurrent in-flight RPCs.</summary>
    public required int MaxInFlight { get; init; }

    /// <summary>When set, allow an <c>http://</c> (h2c) address for local testing. Never used against ACA.</summary>
    public required bool AllowInsecure { get; init; }

    /// <summary>
    /// Parses the command line, returning <see langword="null"/> when required
    /// arguments are missing or malformed (the caller then prints usage).
    /// </summary>
    public static LoadDriverOptions? Parse(string[] args)
    {
        string? target = null;
        var username = "admin";
        string? password = null;
        var rate = 2000.0;
        var durationSeconds = 300.0;
        var treeCount = 64;
        var keySpace = 100_000L;
        var readRatio = 0.2;
        var payloadBytes = 256;
        var maxInFlight = 512;
        var allowInsecure = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--target" or "-t":
                    target = Next(args, ref i);
                    break;
                case "--user" or "-u":
                    username = Next(args, ref i) ?? username;
                    break;
                case "--password" or "-p":
                    password = Next(args, ref i);
                    break;
                case "--rate" or "-r":
                    rate = ParseDouble(Next(args, ref i), rate);
                    break;
                case "--duration" or "-d":
                    durationSeconds = ParseDouble(Next(args, ref i), durationSeconds);
                    break;
                case "--trees":
                    treeCount = (int)ParseDouble(Next(args, ref i), treeCount);
                    break;
                case "--keyspace":
                    keySpace = (long)ParseDouble(Next(args, ref i), keySpace);
                    break;
                case "--read-ratio":
                    readRatio = ParseDouble(Next(args, ref i), readRatio);
                    break;
                case "--payload-bytes":
                    payloadBytes = (int)ParseDouble(Next(args, ref i), payloadBytes);
                    break;
                case "--max-in-flight":
                    maxInFlight = (int)ParseDouble(Next(args, ref i), maxInFlight);
                    break;
                case "--insecure":
                    allowInsecure = true;
                    break;
                default:
                    Console.Error.WriteLine($"Unknown argument: {arg}");
                    return null;
            }
        }

        if (string.IsNullOrWhiteSpace(target) || string.IsNullOrEmpty(password))
        {
            return null;
        }

        if (rate <= 0 || durationSeconds <= 0 || treeCount <= 0 || keySpace <= 0 ||
            payloadBytes < 0 || maxInFlight <= 0 || readRatio < 0 || readRatio > 1)
        {
            Console.Error.WriteLine("One or more numeric arguments are out of range.");
            return null;
        }

        var address = NormaliseAddress(target, allowInsecure);

        return new LoadDriverOptions
        {
            Address = address,
            Username = username,
            Password = password,
            OfferedRatePerSecond = rate,
            Duration = TimeSpan.FromSeconds(durationSeconds),
            TreeCount = treeCount,
            KeySpace = keySpace,
            ReadRatio = readRatio,
            PayloadBytes = payloadBytes,
            MaxInFlight = maxInFlight,
            AllowInsecure = allowInsecure,
        };
    }

    /// <summary>Prints the usage banner to stderr.</summary>
    public static void PrintUsage()
    {
        Console.Error.WriteLine(
            """
            ClusterScaling.LoadDriver - compute-axis load generator for the ACA scaling sample.

            Required:
              --target,   -t <fqdn|url>   Data-API ingress (FQDN or https:// URL).
              --password, -p <password>   Admin password (plaintext; presented as Basic over TLS).

            Optional:
              --user,     -u <name>       Admin username (default: admin).
              --rate,     -r <ops/sec>    Offered operations per second (default: 2000).
              --duration, -d <seconds>    How long to sustain the load (default: 300).
              --trees        <count>      Distinct trees to spread load across (default: 64).
              --keyspace     <count>      Distinct keys per tree cycle (default: 100000).
              --read-ratio   <0..1>       Fraction of ops issued as reads (default: 0.2).
              --payload-bytes <n>         Write payload size in bytes (default: 256).
              --max-in-flight <n>         Max concurrent in-flight RPCs (default: 512).
              --insecure                  Allow an http:// (h2c) target for local testing.
            """);
    }

    private static string NormaliseAddress(string target, bool allowInsecure)
    {
        if (target.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            target.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return target;
        }

        // A bare FQDN defaults to TLS (the ACA ingress). --insecure only affects
        // an explicitly http:// address.
        return (allowInsecure ? "http://" : "https://") + target;
    }

    private static string? Next(string[] args, ref int i)
    {
        if (i + 1 >= args.Length)
        {
            return null;
        }

        i++;
        return args[i];
    }

    private static double ParseDouble(string? value, double fallback) =>
        double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
}
