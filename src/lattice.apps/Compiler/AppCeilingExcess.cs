using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// One way a manifest role exceeds its install capability ceiling. A compilation reporting any
/// excess fails activation as a whole and emits no rules; the excess is never silently clamped.
/// </summary>
/// <param name="RoleName">The manifest role that exceeds the ceiling.</param>
/// <param name="Kind">Whether the role exceeds the operations mask or the approved scope set.</param>
/// <param name="Operations">
/// For <see cref="AppCeilingExcessKind.Operations"/>, only the requested bits missing from the ceiling.
/// For <see cref="AppCeilingExcessKind.Scope"/>, every operation the role requests on that scope.
/// </param>
/// <param name="Scope">
/// For <see cref="AppCeilingExcessKind.Scope"/>, the uncovered scope in the ceiling's tenant-local
/// vocabulary (before tenant composition), so it can be approved verbatim as an exception scope;
/// <c>null</c> for <see cref="AppCeilingExcessKind.Operations"/>.
/// </param>
public sealed record AppCeilingExcess(
    string RoleName,
    AppCeilingExcessKind Kind,
    LatticeOperation Operations,
    LatticeScope? Scope);
