using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;

/// <summary>
/// The persona catalogue (issue #5360): the single source of every persona name in epic
/// #5401's decision record. A journey reads a persona from here and never spells a user
/// name of its own.
/// </summary>
/// <remarks>
/// <para>
/// The catalogue is loaded into F1's identity seam: <see cref="ContosoUsers"/> is the default
/// <see cref="JourneyEstateOptions.Users"/>, and a Fabrikam estate passes
/// <see cref="FabrikamUsers"/> and <see cref="BootstrapAdministratorsOf"/>. Every persona can
/// sign in from boot, but only the bootstrap administrator holds any right: everything else
/// a persona exercises is granted earlier in the story, or by the contract seeders in
/// isolated mode.
/// </para>
/// <para>
/// A password is <c>{id}-journeys-1</c>. The estate's directory is test-owned and the
/// passwords are not secrets; they differ per persona so a credential mix-up fails.
/// </para>
/// </remarks>
public static class Personas
{
    /// <summary>The non-tenanted manufacturing estate that carries the main story.</summary>
    public const string ContosoEstate = "contoso";

    /// <summary>The tenanted hosting provider that carries the tenancy storyline.</summary>
    public const string FabrikamEstate = "fabrikam";

    /// <summary>Contoso's platform operator and bootstrap administrator.</summary>
    public static Persona Olivia { get; } = new(
        JourneyEstateOptions.Administrator, "Olivia Okafor", Password(JourneyEstateOptions.Administrator), ContosoEstate,
        "Platform operator", IsBootstrapAdministrator: true);

    /// <summary>Contoso's data steward (<c>data-stewards</c> from A2 until C3).</summary>
    public static Persona Priya { get; } = new("priya", "Priya Raman", Password("priya"), ContosoEstate, "Data steward");

    /// <summary>Contoso's schema owner (<c>schema-owners</c>).</summary>
    public static Persona Sam { get; } = new("sam", "Sam Ortega", Password("sam"), ContosoEstate, "Schema owner");

    /// <summary>Contoso's auditor (<c>auditors</c>; also a reader through the nested <c>factory-staff</c>).</summary>
    public static Persona Dana { get; } = new("dana", "Dana Whitfield", Password("dana"), ContosoEstate, "Auditor");

    /// <summary>Contoso's new starter, who holds no grant.</summary>
    public static Persona Nina { get; } = new("nina", "Nina Kowalski", Password("nina"), ContosoEstate, "New starter, no grants");

    /// <summary>Contoso's upstream ingestion service (<c>ingestion-services</c>), run by the harness.</summary>
    public static Persona LineFeed { get; } = new(
        JourneyEstateOptions.LineFeedIdentity, "Line feed (ingestion service)", Password(JourneyEstateOptions.LineFeedIdentity), ContosoEstate,
        "Upstream ingestion service", IsService: true);

    /// <summary>Fabrikam's hosting operator and bootstrap administrator.</summary>
    public static Persona Hal { get; } = new("hal", "Hal Brennan", Password("hal"), FabrikamEstate, "Hosting operator", IsBootstrapAdministrator: true);

    /// <summary>The acme tenant's administrator.</summary>
    public static Persona Ada { get; } = new("ada", "Ada Lindqvist", Password("ada"), FabrikamEstate, "Acme tenant administrator", HomeTenant: "acme");

    /// <summary>The acme tenant's analyst.</summary>
    public static Persona Ari { get; } = new("ari", "Ari Mensah", Password("ari"), FabrikamEstate, "Acme analyst", HomeTenant: "acme");

    /// <summary>The globex tenant's administrator.</summary>
    public static Persona Gus { get; } = new("gus", "Gus Ferreira", Password("gus"), FabrikamEstate, "Globex tenant administrator", HomeTenant: "globex");

    /// <summary>The globex tenant's buyer.</summary>
    public static Persona Gia { get; } = new("gia", "Gia Moretti", Password("gia"), FabrikamEstate, "Globex buyer", HomeTenant: "globex");

    /// <summary>A default-tenant user who is not an operator.</summary>
    public static Persona Dee { get; } = new("dee", "Dee Adeyemi", Password("dee"), FabrikamEstate, "Default-tenant user, not an operator", HomeTenant: "default");

    /// <summary>
    /// An acme member with no rule at all (T2), the witness that tenant membership confers no data
    /// rights of its own: every read and write it attempts on acme's trees is refused.
    /// </summary>
    public static Persona Abe { get; } = new("abe", "Abe Novak", Password("abe"), FabrikamEstate, "Acme member, no rules", HomeTenant: "acme");

    /// <summary>The Contoso cast, in the decision record's order.</summary>
    public static IReadOnlyList<Persona> Contoso { get; } = [Olivia, Priya, Sam, Dana, Nina, LineFeed];

    /// <summary>The Fabrikam cast, in the decision record's order.</summary>
    public static IReadOnlyList<Persona> Fabrikam { get; } = [Hal, Ada, Ari, Gus, Gia, Dee, Abe];

    /// <summary>Every persona in the catalogue.</summary>
    public static IReadOnlyList<Persona> All { get; } = [.. Contoso, .. Fabrikam];

    /// <summary>The persona with subject id <paramref name="id"/>.</summary>
    /// <param name="id">The subject id.</param>
    /// <exception cref="ArgumentException">No persona has that id.</exception>
    public static Persona Get(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        return All.FirstOrDefault(persona => string.Equals(persona.Id, id, StringComparison.Ordinal))
            ?? throw new ArgumentException($"'{id}' is not in the persona catalogue.", nameof(id));
    }

    /// <summary>The cast of <paramref name="estate"/>.</summary>
    /// <param name="estate"><see cref="ContosoEstate"/> or <see cref="FabrikamEstate"/>.</param>
    /// <exception cref="ArgumentException">The estate is neither.</exception>
    public static IReadOnlyList<Persona> Of(string estate) => estate switch
    {
        ContosoEstate => Contoso,
        FabrikamEstate => Fabrikam,
        _ => throw new ArgumentException($"'{estate}' is not an estate of the story.", nameof(estate)),
    };

    /// <summary>
    /// A fresh, mutable directory user list for a Contoso estate: the default
    /// <see cref="JourneyEstateOptions.Users"/>. Fresh on every call, because
    /// <see cref="JourneyEstate.AddUser"/> adds to it.
    /// </summary>
    public static IList<JourneyUser> ContosoUsers() => [.. Contoso.Select(persona => persona.ToUser())];

    /// <summary>A fresh, mutable directory user list for a Fabrikam estate.</summary>
    public static IList<JourneyUser> FabrikamUsers() => [.. Fabrikam.Select(persona => persona.ToUser())];

    /// <summary>A fresh list of the bootstrap administrators of <paramref name="estate"/>.</summary>
    /// <param name="estate"><see cref="ContosoEstate"/> or <see cref="FabrikamEstate"/>.</param>
    public static IList<string> BootstrapAdministratorsOf(string estate) =>
        [.. Of(estate).Where(persona => persona.IsBootstrapAdministrator).Select(persona => persona.Id)];

    private static string Password(string id) => id + "-journeys-1";
}
