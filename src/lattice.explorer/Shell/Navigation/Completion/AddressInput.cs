namespace Orleans.Lattice.Explorer.Shell.Navigation.Completion;

/// <summary>
/// What the user typed into the address line, read as a mode and the text to
/// complete: <c>&gt;</c> is the command palette, <c>t/</c> a tenant, <c>a/</c> an
/// app, a leading <c>/</c> a literal address, and anything else a free search.
/// </summary>
/// <param name="Mode">The mode the prefix selects.</param>
/// <param name="Text">The text after the prefix, trimmed; for <see cref="AddressQueryMode.Address"/> the raw input.</param>
internal readonly record struct AddressInput(AddressQueryMode Mode, string Text)
{
    /// <summary>The prefix that opens the command palette.</summary>
    public const string CommandPrefix = ">";

    /// <summary>The prefix that completes a tenant.</summary>
    public const string TenantPrefix = "t/";

    /// <summary>The prefix that completes an app.</summary>
    public const string AppPrefix = "a/";

    /// <summary>Reads raw input.</summary>
    /// <param name="raw">The input, or <see langword="null"/> for none.</param>
    public static AddressInput Read(string? raw)
    {
        var text = (raw ?? string.Empty).TrimStart();

        if (text.StartsWith(CommandPrefix, StringComparison.Ordinal))
        {
            return new AddressInput(AddressQueryMode.Command, text[CommandPrefix.Length..].Trim());
        }

        if (text.StartsWith(TenantPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return new AddressInput(AddressQueryMode.Tenant, text[TenantPrefix.Length..].Trim());
        }

        if (text.StartsWith(AppPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return new AddressInput(AddressQueryMode.App, text[AppPrefix.Length..].Trim());
        }

        if (text.StartsWith('/'))
        {
            return new AddressInput(AddressQueryMode.Address, text.TrimEnd());
        }

        return new AddressInput(AddressQueryMode.Search, text.Trim());
    }
}
