using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;

/// <summary>
/// The persona catalogue (#5360) holds exactly the cast of epic #5401's decision record, and
/// is what the estate's identity directory is loaded with.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class PersonaCatalogueTests
{
    /// <summary>The Contoso cast is the decision record's, in its order, and only olivia administers at boot.</summary>
    [Test]
    public void The_contoso_cast_is_the_decision_records()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Personas.Contoso.Select(persona => persona.Id), Is.EqualTo(new[] { "olivia", "priya", "sam", "dana", "nina", "line-feed" }));
            Assert.That(Personas.Contoso.Select(persona => persona.Estate), Has.All.EqualTo(Personas.ContosoEstate));
            Assert.That(Personas.BootstrapAdministratorsOf(Personas.ContosoEstate), Is.EqualTo(new[] { JourneyEstateOptions.Administrator }));
            Assert.That(Personas.Contoso.Where(persona => persona.IsService).Select(persona => persona.Id), Is.EqualTo(new[] { JourneyEstateOptions.LineFeedIdentity }));
        });
    }

    /// <summary>The Fabrikam cast is the decision record's, in its order, with hal its bootstrap administrator.</summary>
    [Test]
    public void The_fabrikam_cast_is_the_decision_records()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Personas.Fabrikam.Select(persona => persona.Id), Is.EqualTo(new[] { "hal", "ada", "ari", "gus", "gia", "dee" }));
            Assert.That(Personas.Fabrikam.Select(persona => persona.Estate), Has.All.EqualTo(Personas.FabrikamEstate));
            Assert.That(Personas.BootstrapAdministratorsOf(Personas.FabrikamEstate), Is.EqualTo(new[] { "hal" }));
            Assert.That(Personas.Fabrikam.Select(persona => persona.HomeTenant), Is.EqualTo(new[] { null, "acme", "acme", "globex", "globex", "default" }));
        });
    }

    /// <summary>Every persona has its own id and its own password, so a credential mix-up fails.</summary>
    [Test]
    public void Every_persona_has_a_distinct_id_and_password()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Personas.All.Select(persona => persona.Id), Is.Unique);
            Assert.That(Personas.All.Select(persona => persona.Password), Is.Unique);
            Assert.That(Personas.All.Select(persona => persona.DisplayName), Is.Unique);
            Assert.That(Personas.All, Has.Count.EqualTo(12));
        });
    }

    /// <summary>An estate's directory is the Contoso cast unless told otherwise.</summary>
    [Test]
    public void The_estate_directory_defaults_to_the_contoso_cast()
    {
        var options = new JourneyEstateOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Users, Is.EqualTo(Personas.Contoso.Select(persona => persona.ToUser())));
            Assert.That(options.BootstrapAdministrators, Is.EqualTo(Personas.BootstrapAdministratorsOf(Personas.ContosoEstate)));
            Assert.That(options.Users, Is.Not.SameAs(new JourneyEstateOptions().Users), "each estate gets a list of its own, because AddUser adds to it");
            Assert.That(Personas.FabrikamUsers().Select(user => user.Id), Is.EqualTo(Personas.Fabrikam.Select(persona => persona.Id)));
        });
    }

    /// <summary>A persona is found by id, and an unknown id or estate is refused.</summary>
    [Test]
    public void A_persona_is_found_by_id_and_unknown_names_are_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Personas.Get("nina"), Is.SameAs(Personas.Nina));
            Assert.That(Personas.Of(Personas.FabrikamEstate), Is.SameAs(Personas.Fabrikam));
            Assert.That(Personas.Nina.ToString(), Is.EqualTo("nina"));
            Assert.That(() => Personas.Get("mallory"), Throws.ArgumentException);
            Assert.That(() => Personas.Of("initech"), Throws.ArgumentException);
            Assert.That(() => Personas.Get(null!), Throws.ArgumentNullException);
        });
    }
}
