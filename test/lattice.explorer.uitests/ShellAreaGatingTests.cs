using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Asserts that the <b>real</b> registered plugin set gates correctly in a real
/// browser: which areas the shell offers a signed-out, disconnected caller, and
/// which it withholds.
/// </summary>
/// <remarks>
/// <para>
/// This closes a genuine seam. Each access gate is tested in isolation against a
/// fake operations surface, and the shell's four-state rendering contract is
/// tested against synthetic plugins - but nothing joined the two. Every existing
/// shell test constructs its own <c>Plugin("a", "Alpha", ...)</c> stubs, so no
/// test asserted that the plugins the application actually registers, wired to
/// the gates they actually declare, produce the area strip a user sees. A plugin
/// wired to the wrong gate, or a gate that admits when it should not, passes
/// every existing test.
/// </para>
/// <para>
/// The assertion direction that matters is <b>withholding</b>. An area appearing
/// when it should not is a fail-open: the shell offers a surface whose data the
/// caller may not read, and the failure is only caught later and deeper. So the
/// load-bearing case here is Telemetry, whose gate reports <i>unavailable</i>
/// while the shell has no usable connection - and the shell renders an
/// unavailable area as no entry at all, rather than as a greyed-out one.
/// </para>
/// <para>
/// This is deliberately the browser tier rather than bUnit. The subject is not
/// one component's behaviour - that is already covered - but the composition:
/// real dependency injection, the real plugin catalogue, real gate probes over a
/// real (disconnected) connection, and the real render. Only a running host
/// exercises that.
/// </para>
/// <para>
/// Related: the server-side half of telemetry authorization is #1795 / #1798,
/// whose Explorer-visible symptom was exactly a Telemetry area that did not
/// appear. This guards the client-side half of that contract.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class ShellAreaGatingTests : UiTestBase
{
    private const int Width = 1400;
    private const int Height = 900;

    /// <summary>
    /// The always-available area. Selection is the one surface that does not
    /// depend on a cluster round trip, so it must survive a disconnected probe -
    /// otherwise a disconnected shell would offer nothing at all.
    /// </summary>
    private const string AlwaysOfferedArea = "Explore";

    /// <summary>
    /// Areas whose gates cannot admit without a usable connection. Each is
    /// withheld for its own reason, but the observable contract is the same:
    /// no tab at all, not a disabled one.
    /// </summary>
    private static readonly string[] ConnectionDependentAreas =
    [
        "Telemetry",
    ];

    [Test]
    public async Task A_disconnected_shell_still_offers_the_selection_area()
    {
        // Guards the opposite failure from the one below: a gate sweep that
        // withheld everything would satisfy "Telemetry is absent" while leaving
        // the shell useless. Something must still be offered.
        var page = await OpenHomeAsync(Width, Height);

        var tabs = page.Locator("[role=tab]");
        await Assertions.Expect(tabs.First).ToBeAttachedAsync();

        await Assertions
            .Expect(page.GetByRole(AriaRole.Tab, new() { Name = AlwaysOfferedArea, Exact = true }))
            .ToBeVisibleAsync();
    }

    [Test]
    public async Task A_disconnected_shell_withholds_every_connection_dependent_area()
    {
        var page = await OpenHomeAsync(Width, Height);

        // Assert the strip rendered before asserting anything is missing from it.
        // Without this, a shell that failed to render at all would satisfy every
        // absence assertion below and the test would pass at its least useful.
        await Assertions.Expect(page.Locator("[role=tab]").First).ToBeAttachedAsync();

        foreach (var area in ConnectionDependentAreas)
        {
            // Count, not visibility: the contract is that an unavailable area
            // renders no entry whatsoever. A disabled-but-present tab would be
            // the denial rendering, which is a different state and would mean
            // the gate classified a missing connection as a refusal of the
            // caller - the exact conflation the telemetry gate documents that it
            // must never make.
            await Assertions
                .Expect(page.GetByRole(AriaRole.Tab, new() { Name = area, Exact = true }))
                .ToHaveCountAsync(0);
        }
    }
}
