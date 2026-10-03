using System.Reflection;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Enters the core's internal system-origin scope from a test assembly that has no
/// <c>InternalsVisibleTo</c> into the core library. System origin is the most
/// privileged origin an operator path runs under, and the reserved
/// <c>sys-membership-*</c> trees refuse raw writes from any other origin.
/// </summary>
internal static class SystemOriginScope
{
    private static readonly MethodInfo EnterMethod =
        typeof(LatticeSubject).Assembly
            .GetType("Orleans.Lattice.LatticeAccessGateContext", throwOnError: true)!
            .GetMethod("EnterSystemOrigin", BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("LatticeAccessGateContext.EnterSystemOrigin was not found.");

    /// <summary>Enters system origin until the returned scope is disposed.</summary>
    /// <returns>The scope to dispose.</returns>
    internal static IDisposable Enter() => (IDisposable)EnterMethod.Invoke(null, null)!;
}
