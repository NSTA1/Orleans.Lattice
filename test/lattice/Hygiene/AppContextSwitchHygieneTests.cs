using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Holds the line against the inert unencrypted-HTTP/2 app switch returning to
/// the samples and the reference architecture (issue #1796). The scan logic
/// lives in the shared base; this fixture binds the scope and remediation for
/// the runnable hosts, which are not part of any package slice.
/// </summary>
/// <remarks>
/// Nine sample and reference-architecture hosts set the switch before creating
/// a gRPC channel. On .NET 10 it does nothing: measured on 10.0.11 against a
/// Kestrel host bound to <c>HttpProtocols.Http2</c> on a plaintext loopback
/// port, h2c succeeded identically with the switch never set, explicitly
/// <see langword="false"/>, and set <see langword="true"/>. It was a .NET Core
/// 3.x affordance. Removing it changed no transport behaviour; what selects
/// plaintext is binding Kestrel without a certificate, and what makes the
/// client speak it is grpc-dotnet's h2c-by-prior-knowledge over an
/// <c>http://</c> address. The samples are the first thing a reader copies, so
/// the switch reappearing there teaches process-global state that nothing
/// needs.
/// </remarks>
[TestFixture]
public sealed class AppContextSwitchHygieneTests : AppContextSwitchHygieneTestsBase
{
    /// <inheritdoc />
    protected override IReadOnlyList<string> ScanRoots { get; } = ["samples", "reference-architecture"];

    /// <inheritdoc />
    protected override string RemediationHint { get; } =
        "Delete the call: bind Kestrel to HttpProtocols.Http2 with no certificate and dial an "
        + "http:// address, which is all h2c needs. Where the h2c choice deserves an explanation, "
        + "put it in a comment that describes the binding rather than attributing it to a switch.";
}
