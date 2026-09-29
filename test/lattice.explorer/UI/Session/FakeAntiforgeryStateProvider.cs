using Microsoft.AspNetCore.Components.Forms;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>Supplies a fixed antiforgery token, so a form-post control renders its hidden field.</summary>
internal sealed class FakeAntiforgeryStateProvider : AntiforgeryStateProvider
{
    /// <summary>The hidden field's name.</summary>
    public const string FieldName = "__RequestVerificationToken";

    /// <summary>The token value.</summary>
    public const string Token = "test-antiforgery-token";

    /// <inheritdoc />
    public override AntiforgeryRequestToken? GetAntiforgeryToken() => new(Token, FieldName);
}
