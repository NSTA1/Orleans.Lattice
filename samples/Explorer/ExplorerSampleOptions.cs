using System.Diagnostics.CodeAnalysis;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// How the sample runs: the full two-region estate with tenancy (the default),
/// or <c>--minimal</c> for the single-cluster experience; which region the
/// Explorer connects to; and the identity-directory and group-merge settings
/// read from the environment.
/// </summary>
internal sealed class ExplorerSampleOptions
{
    /// <summary>The switch that keeps the single-cluster experience.</summary>
    public const string MinimalSwitch = "--minimal";

    /// <summary>The switch that picks the region the Explorer connects to; it takes <c>east</c> or <c>west</c>.</summary>
    public const string ExplorerRegionSwitch = "--explorer-region";

    /// <summary>The switch that pauses the link to the peer region as soon as the seeded data has reached it.</summary>
    public const string PeerPausedSwitch = "--peer-paused";

    /// <summary>The switch that picks the identity the console signs in as automatically, or <c>none</c>.</summary>
    public const string SignInAsSwitch = "--sign-in-as";

    /// <summary>The <see cref="SignInAsSwitch"/> value that turns the automatic sign-in off.</summary>
    public const string NoSignIn = "none";

    /// <summary>The switch that shifts every port by a number, for when the default ports are taken.</summary>
    public const string PortOffsetSwitch = "--port-offset";

    /// <summary>The largest <see cref="PortOffsetSwitch"/> accepted, which keeps every port below 65536.</summary>
    public const int MaxPortOffset = 30000;

    /// <summary>The Entra tenant variable; set it with the other two to use the Entra identity directory.</summary>
    public const string EntraTenantIdVariable = "LATTICE_ENTRA_TENANT_ID";

    /// <summary>The Entra client-id variable.</summary>
    public const string EntraClientIdVariable = "LATTICE_ENTRA_CLIENT_ID";

    /// <summary>The Entra client-secret variable.</summary>
    public const string EntraClientSecretVariable = "LATTICE_ENTRA_CLIENT_SECRET";

    /// <summary>The group-merge mode variable: <c>Union</c> (default), <c>TokenOnly</c> or <c>DirectoryOnly</c>.</summary>
    public const string MergeModeVariable = "LATTICE_MEMBERSHIP_MERGE_MODE";

    /// <summary>The usage line printed with a rejected argument.</summary>
    public const string Usage = "Usage: dotnet run [-- [--minimal] [--explorer-region east|west] [--sign-in-as <user>|none] [--peer-paused] [--port-offset <n>]]";

    /// <summary>Whether to run one single-region cluster without tenancy, as the sample did before the estate.</summary>
    public bool Minimal { get; init; }

    /// <summary>The region the Explorer console connects to.</summary>
    public string ExplorerRegion { get; init; } = SampleIdentities.EastRegion;

    /// <summary>
    /// The identity the console signs in as automatically, or <see langword="null"/>
    /// to leave it signed out so the sign-in dialog picks the identity. Signing out
    /// does not stick while an automatic sign-in is set: the console signs straight
    /// back in.
    /// </summary>
    public string? SignInAs { get; init; } = SampleIdentities.Administrator;

    /// <summary>Whether to pause the link to the peer region once the seeded data has reached it.</summary>
    public bool StartPeerPaused { get; init; }

    /// <summary>The Entra registration backing the identity directory, or <see langword="null"/> for the static roster.</summary>
    public SampleEntraDirectory? Entra { get; init; }

    /// <summary>Whether locally-defined membership contributes to authorization.</summary>
    public SubjectGroupMergeMode GroupMergeMode { get; init; } = SubjectGroupMergeMode.Union;

    /// <summary>The ports the sample binds.</summary>
    public SamplePorts Ports { get; init; } = SamplePorts.Default;

    /// <summary>
    /// The Explorer console's persisted configuration file. The sample deletes it
    /// on start, so every run connects to the region it was asked for.
    /// </summary>
    public string ExplorerConfigPath { get; init; } = Path.Combine(AppContext.BaseDirectory, "explorer-sample-config.json");

    /// <summary>How often the background writer updates one replicated key.</summary>
    public TimeSpan WriterInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Reads the command line and the environment, rejecting anything it does not
    /// recognise so a typo never silently runs a different sample.
    /// </summary>
    /// <param name="args">The command-line arguments.</param>
    /// <param name="environment">Reads an environment variable, returning <see langword="null"/> when it is unset.</param>
    /// <param name="options">The options, when the input is valid.</param>
    /// <param name="error">Why the input was rejected, when it is not.</param>
    /// <returns>Whether the input was valid.</returns>
    public static bool TryParse(
        IReadOnlyList<string> args,
        Func<string, string?> environment,
        [NotNullWhen(true)] out ExplorerSampleOptions? options,
        [NotNullWhen(false)] out string? error)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(environment);

        options = null;
        var minimal = false;
        var peerPaused = false;
        var portOffset = 0;
        string? region = null;
        var signInAs = SampleIdentities.Administrator;

        for (var i = 0; i < args.Count; i++)
        {
            var arg = args[i];
            if (string.Equals(arg, MinimalSwitch, StringComparison.OrdinalIgnoreCase))
            {
                minimal = true;
            }
            else if (string.Equals(arg, PeerPausedSwitch, StringComparison.OrdinalIgnoreCase))
            {
                peerPaused = true;
            }
            else if (string.Equals(arg, ExplorerRegionSwitch, StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Count)
                {
                    error = $"{ExplorerRegionSwitch} needs a region: {SampleIdentities.EastRegion} or {SampleIdentities.WestRegion}.";
                    return false;
                }

                region = args[++i].Trim().ToLowerInvariant();
                if (region is not (SampleIdentities.EastRegion or SampleIdentities.WestRegion))
                {
                    error = $"{ExplorerRegionSwitch} '{args[i]}' is not a region: use {SampleIdentities.EastRegion} or {SampleIdentities.WestRegion}.";
                    return false;
                }
            }
            else if (string.Equals(arg, SignInAsSwitch, StringComparison.OrdinalIgnoreCase))
            {
                var value = i + 1 < args.Count ? args[++i].Trim() : string.Empty;
                if (value.Length == 0 || value.StartsWith('-') || value.Contains(':', StringComparison.Ordinal) || value.Any(char.IsWhiteSpace))
                {
                    error = $"{SignInAsSwitch} needs a user name, such as {SampleIdentities.AcmeAdmin}, or {NoSignIn}.";
                    return false;
                }

                signInAs = value;
            }
            else if (string.Equals(arg, PortOffsetSwitch, StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Count
                    || !int.TryParse(args[++i], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out portOffset)
                    || portOffset > MaxPortOffset)
                {
                    error = $"{PortOffsetSwitch} needs a whole number from 0 to {MaxPortOffset}.";
                    return false;
                }
            }
            else
            {
                error = $"'{arg}' is not recognised. {Usage}";
                return false;
            }
        }

        if (minimal && (peerPaused || region == SampleIdentities.WestRegion))
        {
            error = $"{MinimalSwitch} runs one region with no peer, so it cannot be combined with {PeerPausedSwitch} or {ExplorerRegionSwitch} {SampleIdentities.WestRegion}.";
            return false;
        }

        if (!TryReadEntra(environment, out var entra, out var entraError))
        {
            error = entraError;
            return false;
        }

        var mergeModeVariable = environment(MergeModeVariable);
        var mergeMode = SubjectGroupMergeMode.Union;
        if (!string.IsNullOrWhiteSpace(mergeModeVariable)
            && (!Enum.TryParse(mergeModeVariable, ignoreCase: true, out mergeMode) || !Enum.IsDefined(mergeMode)))
        {
            error = $"{MergeModeVariable} '{mergeModeVariable}' is not recognised. Set it to Union, TokenOnly or DirectoryOnly, or unset it to use Union.";
            return false;
        }

        options = new ExplorerSampleOptions
        {
            Minimal = minimal,
            ExplorerRegion = region ?? SampleIdentities.EastRegion,
            SignInAs = string.Equals(signInAs, NoSignIn, StringComparison.OrdinalIgnoreCase) ? null : signInAs,
            StartPeerPaused = peerPaused,
            Entra = entra,
            GroupMergeMode = mergeMode,
            Ports = SamplePorts.Default.Offset(portOffset),
        };
        error = null;
        return true;
    }

    // Half-configuring Entra is rejected, so a partial configuration never
    // silently degrades to the static directory.
    private static bool TryReadEntra(
        Func<string, string?> environment,
        out SampleEntraDirectory? entra,
        [NotNullWhen(false)] out string? error)
    {
        var tenantId = environment(EntraTenantIdVariable);
        var clientId = environment(EntraClientIdVariable);
        var clientSecret = environment(EntraClientSecretVariable);
        var set = (string.IsNullOrWhiteSpace(tenantId) ? 0 : 1)
            + (string.IsNullOrWhiteSpace(clientId) ? 0 : 1)
            + (string.IsNullOrWhiteSpace(clientSecret) ? 0 : 1);

        entra = set == 3 ? new SampleEntraDirectory(tenantId!, clientId!, clientSecret!) : null;
        if (set is > 0 and < 3)
        {
            error = $"Entra directory mode is half-configured. Set all of {EntraTenantIdVariable}, {EntraClientIdVariable} and {EntraClientSecretVariable} to back the Access directory with your Entra tenant, or unset all three to use the built-in static directory. See samples/Explorer/README.md.";
            return false;
        }

        error = null;
        return true;
    }
}
