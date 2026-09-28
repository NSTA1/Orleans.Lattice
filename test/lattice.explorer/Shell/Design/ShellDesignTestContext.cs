using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design;

/// <summary>
/// The bUnit context the Shell design primitives are tested under: the design
/// system's own services registered as the Shell registers them, and JSInterop
/// in loose mode so an incidental interop call (a focus request) never faults a
/// render. A test that asserts on an interop call sets it up or verifies it.
/// </summary>
/// <remarks>
/// Assert against the parsed DOM (<c>GetAttribute</c>, <c>Find</c>), never
/// against raw markup, so a bare boolean attribute reads as a browser reports
/// it. Every interaction is dispatched explicitly by the test; nothing here
/// waits on a timer.
/// </remarks>
public abstract class ShellDesignTestContext : BunitContext
{
    /// <summary>Registers the design services and loosens JSInterop.</summary>
    protected ShellDesignTestContext()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
        Services.AddScoped<LtToastService>();
    }
}
