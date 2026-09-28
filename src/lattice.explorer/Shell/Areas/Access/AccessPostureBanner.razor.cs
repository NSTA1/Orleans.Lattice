using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// The access-state banner: the cluster's authentication and enforcement
/// posture, its opt-in authorization tiers and its identity directory, plus a
/// notice when rules are not enforced or local membership has no effect.
/// </summary>
public partial class AccessPostureBanner
{
    /// <summary>The cluster's access model, or <see langword="null"/> when it is unknown.</summary>
    [Parameter]
    public AccessModelDescriptor? Model { get; set; }

    private static string OnOff(bool value) => value ? "On" : "Off";

    private static string AuthenticationLabel(AccessAuthenticationMode mode) => mode switch
    {
        AccessAuthenticationMode.Anonymous => "Anonymous",
        AccessAuthenticationMode.Claims => "Claims (token)",
        AccessAuthenticationMode.Basic => "Basic (username and password)",
        _ => "Unknown",
    };
}
