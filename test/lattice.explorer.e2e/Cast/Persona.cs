using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;

/// <summary>
/// One person, or one service, in the journeys' cast (epic #5401's decision record): who
/// they are, which estate they belong to, and how they sign in.
/// </summary>
/// <param name="Id">The subject id, which is also the sign-in user name.</param>
/// <param name="DisplayName">The name the identity directory publishes.</param>
/// <param name="Password">The password the estate's silos check.</param>
/// <param name="Estate">The estate the persona belongs to: <see cref="Personas.ContosoEstate"/> or <see cref="Personas.FabrikamEstate"/>.</param>
/// <param name="Role">The persona's part in the story, in words.</param>
/// <param name="IsBootstrapAdministrator">
/// Whether the persona is the estate's bootstrap administrator: the only identity that holds
/// any right when the estate boots. Every other right is granted earlier in the story.
/// </param>
/// <param name="IsService">Whether the persona is a service identity rather than a person.</param>
/// <param name="HomeTenant">The tenant the persona works in on a tenanted estate, or <see langword="null"/>.</param>
public sealed record Persona(
    string Id,
    string DisplayName,
    string Password,
    string Estate,
    string Role,
    bool IsBootstrapAdministrator = false,
    bool IsService = false,
    string? HomeTenant = null)
{
    /// <summary>The persona as an entry of the estate's identity directory.</summary>
    public JourneyUser ToUser() => new(Id, DisplayName, Password);

    /// <inheritdoc />
    public override string ToString() => Id;
}
