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

    /// <summary>
    /// The Orleans cluster id.
    /// <para>
    /// Defaults to the repocontext host's own default rather than Orleans'
    /// conventional <c>dev</c>. A mismatch is not a soft failure: the gateway
    /// accepts the TCP connection and then rejects the handshake with
    /// <c>Unexpected cluster id</c>, which reads like a networking fault but is
    /// purely an identity mismatch.
    /// </para>
    /// </summary>
    public string ClusterId { get; private set; } = "repo-context";

    /// <summary>The Orleans service id. See <see cref="ClusterId"/>.</summary>
    public string ServiceId { get; private set; } = "repo-context";

    /// <summary>The client response deadline.</summary>
    public TimeSpan ResponseTimeout { get; private set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to keep retrying the initial cluster join.</summary>
    public TimeSpan ConnectTimeout { get; private set; } = TimeSpan.FromMinutes(5);

    /// <summary>How many fleet operations to run at once during create/teardown.</summary>
    public int Parallelism { get; private set; } = 8;

    /// <summary>An in-flight ceiling for the load driver, or zero for none.</summary>
    public int MaxInFlight { get; private set; }

    /// <summary>
    /// The percentage of probe calls issued as <c>GetAllTreeIdsAsync</c> rather
    /// than as point reads. Defaults to zero.
    /// </summary>
    /// <remarks>
    /// Zero keeps the probe a faithful replica of the observed storm mix, which
    /// is entirely point reads. Raising it deliberately changes the question
    /// being asked, because unlike every point read on the interface this member
    /// is not <c>[AlwaysInterleave]</c> and holds the registry singleton's turn
    /// token for a whole multi-hop traversal of the backing tree.
    /// </remarks>
    public int EnumeratePercent { get; private set; }

    /// <summary>
    /// The target leaf count per tree for the <c>populate</c> verb - the DEPTH
    /// axis, varied independently of tree count.
    /// </summary>
    public int LeavesPerTree { get; private set; } = 64;

    /// <summary>
    /// Keys per leaf. Defaults to the tree's own leaf capacity
    /// (<c>LatticeConstants.DefaultMaxLeafKeys</c>), so a leaf target converts
    /// to an entry count without the caller having to know the tree's shape.
    /// </summary>
    public int KeysPerLeaf { get; private set; } = 128;

    /// <summary>
    /// The size of each populated value. The default of 1,600 bytes times the
    /// 128-key leaf capacity reproduces the live estate's ~200 KB leaf snapshot,
    /// which is the unit the cold-start replay actually reads.
    /// </summary>
    public int ValueBytes { get; private set; } = 1600;

    /// <summary>How many entries to send per populate write call.</summary>
    public int BatchSize { get; private set; } = 256;

    /// <summary>
    /// How many distinct trees one <c>fanout</c> wave resolves simultaneously -
    /// the OFFERED fan-in, and the only knob on the rig that moves the quantity
    /// the gate bounds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The existing arms cannot move it at any setting. <c>probe</c> addresses
    /// <c>ILatticeRegistry</c> as an Orleans client, so none of its traffic
    /// passes through a silo-side gate at all; <c>load</c> reaches the resolver
    /// but its trees are warm and few. The birth arm does reach the gate, but its
    /// arrivals are reminder-birthed on a 60-second due time, so K trees offer
    /// only about K/60 distinct resolutions per second: the dispersal grows
    /// exactly as fast as the load, and no tree count closes the gap.
    /// </para>
    /// <para>
    /// This switch attacks the arrival process instead of the estate size. A
    /// wave releases every tree's resolution from one barrier, so the offered
    /// fan-in is the wave width by construction rather than a hoped-for
    /// coincidence of timing.
    /// </para>
    /// </remarks>
    public int FanoutWidth { get; private set; } = 256;

    /// <summary>How many <c>fanout</c> waves to issue.</summary>
    public int FanoutWaves { get; private set; } = 20;

    /// <summary>
    /// Milliseconds to pause between <c>fanout</c> waves. Zero runs them
    /// back-to-back, which is the saturating setting; a positive gap lets the
    /// gate drain between waves and is how the arm is walked DOWN towards the
    /// unsaturated regime to show that the instruments follow it.
    /// </summary>
    public int FanoutGapMillis { get; private set; }

    /// <summary>
    /// When true, the <c>fanout</c> arm bypasses <see cref="ILattice"/> and
    /// issues the same wave straight at <see cref="ILatticeRegistry"/> from the
    /// client, which reaches no silo-side gate.
    /// </summary>
    /// <remarks>
    /// This is the rig's discriminator, and it is the reason the arm is worth
    /// having. A rig that reaches the bounded regime but produces the same
    /// numbers with and without the bound has measured the workload rather than
    /// the bound. Running the identical wave width through both paths makes the
    /// difference the reading, so "the gate is doing something" is observed
    /// rather than assumed.
    /// </remarks>
    public bool FanoutUngated { get; private set; }

    /// <summary>Where to write the JSON report.</summary>
    public string? OutputPath { get; private set; }

    /// <summary>
    /// The subject id the driver presents. Defaults to the repocontext host's
    /// bootstrap administrator, which the default-deny gate short-circuits as
    /// Allow on every tree - so the driver never has to seed policy for its own
    /// trees, and therefore never writes to the reserved policy tree as a side
    /// effect of measuring.
    /// </summary>
    public string Subject { get; private set; } = DriverCredential.BootstrapAdministrator;

    /// <summary>The credential scheme the driver presents.</summary>
    public string Scheme { get; private set; } = DriverCredential.Scheme;

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
            case "--enumerate-pct": options.EnumeratePercent = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--leaves-per-tree": options.LeavesPerTree = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--keys-per-leaf": options.KeysPerLeaf = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--value-bytes": options.ValueBytes = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--batch-size": options.BatchSize = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--fanout-width": options.FanoutWidth = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--fanout-waves": options.FanoutWaves = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--fanout-gap-ms": options.FanoutGapMillis = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                case "--fanout-ungated": options.FanoutUngated = true; break;
                case "--out": options.OutputPath = Value(); break;
                case "--subject": options.Subject = Value(); break;
                case "--scheme": options.Scheme = Value(); break;
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
          populate  Write each fleet tree up to a target LEAF count with
                    realistically-sized values. Leaf count is a controlled axis:
                    an unpopulated estate has ~2 orders of magnitude less state
                    to fault in at cold start than the live one, so a storm that
                    is driven by snapshot replay rather than registry fan-in
                    would not appear at ANY tree count.
          census    Report entry and implied leaf counts across the fleet.
          load      Drive synthetic read/write load against a subset of the fleet.
          probe     Drive registry point reads (Resolve / GetEntry / GetShardMap),
                    mirroring the member mix seen in the observed storm.
          fanout    Release W distinct trees' option resolutions from ONE barrier,
                    repeatedly. This is the only arm that moves silo-side fan-in:
                    'probe' addresses the registry as a client and reaches no
                    silo-side gate at all, 'load' works a small warm set, and the
                    birth arm's arrivals are reminder-dispersed over 60s so its
                    offered fan-in is ~K/60 per second however large K grows.
                    Pair it with --fanout-ungated to run the same wave against the
                    registry directly, which is what makes the arm a DISCRIMINATOR
                    rather than just a bigger load.
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
          --cluster-id ID      Orleans cluster id (default repo-context)
          --service-id ID      Orleans service id (default repo-context)
          --response-timeout S client response deadline (default 30)
          --connect-timeout S  how long to retry the initial join (default 300)
          --parallelism N      concurrent create/teardown operations (default 8)
          --max-in-flight N    in-flight ceiling for load, 0 = none (default 0)
          --enumerate-pct N  share of probe calls issued as GetAllTreeIdsAsync (default 0).
                             Zero replays the observed storm mix, which is entirely
                             point reads and so entirely [AlwaysInterleave] members.
                             Raising it reaches the non-interleaved range scan, which
                             holds the registry singleton turn token throughout.
          --leaves-per-tree N  populate target, leaves per tree (default 64)
          --keys-per-leaf N    tree leaf capacity (default 128)
          --value-bytes N      populated value size (default 1600)
          --batch-size N       entries per populate write call (default 256)
          --fanout-width W     distinct trees released together per wave (default 256).
                               This is the offered fan-in. It must clear the gate's
                               permit count before ANY gate reading is evidence, so
                               treat a wave narrower than that as an unmeasured run
                               rather than a comfortable one.
          --fanout-waves N     waves to issue (default 20)
          --fanout-gap-ms N    pause between waves, 0 = back-to-back (default 0).
                               Raise it to walk the arm DOWN out of saturation and
                               show the instruments follow the regime rather than
                               reporting a constant.
          --fanout-ungated     issue the wave straight at ILatticeRegistry from the
                               client instead of through ILattice, reaching no
                               silo-side gate. The A/B control arm.
          --subject S          credential subject (default repocontext-bootstrap-admin)
          --scheme S           credential scheme (default repocontext-local)
          --out PATH           write the JSON report here
        """;
}
