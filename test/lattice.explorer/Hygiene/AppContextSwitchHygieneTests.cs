using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Hygiene;

/// <summary>
/// The Explorer decides unencrypted-HTTP/2 transport per channel, never per
/// process. This gate holds that line at the source level (issue #1784). The
/// scan logic lives in the shared base; this fixture binds the Explorer's scope
/// and its remediation.
/// </summary>
/// <remarks>
/// <para>
/// Seven gRPC client factories used to set the process-global
/// unencrypted-HTTP/2 app switch from inside a <em>per-circuit</em> channel
/// factory, gated on that circuit's endpoint having opted into unencrypted
/// transport. The switch is process-global and effectively write-once, so one
/// circuit connecting to an <c>http://</c> endpoint decided the posture for
/// every subsequent channel in the process - including circuits that never
/// opted in. On the Blazor Server head, where circuits are per-browser and
/// share a process, that is one operator's choice leaking onto every other
/// operator. Channel construction now lives once in
/// <see cref="LatticeGrpcChannelFactory"/>, which scopes the transport handler
/// to the single channel that asked for it.
/// </para>
/// <para>
/// Only <c>src/lattice.explorer</c> is scanned: the Explorer's own transport
/// tests name the switch in code precisely to assert that it is never set, and
/// must not be read as violations.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AppContextSwitchHygieneTests : AppContextSwitchHygieneTestsBase
{
    /// <inheritdoc />
    protected override IReadOnlyList<string> ScanRoots { get; } = ["src/lattice.explorer"];

    /// <inheritdoc />
    protected override string RemediationHint { get; } =
        $"Build the channel through {nameof(LatticeGrpcChannelFactory)} instead, which scopes the "
        + "transport handler to the one channel that asked for it.";
}
