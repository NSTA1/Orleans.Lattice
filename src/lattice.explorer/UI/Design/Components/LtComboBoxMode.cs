namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>What a combobox accepts.</summary>
public enum LtComboBoxMode
{
    /// <summary>
    /// Only a value the source lists is accepted; a typed value that matches
    /// nothing is refused with an inline error. Use it where the field names an
    /// existing tree, region, principal or tenant.
    /// </summary>
    PickExisting,

    /// <summary>
    /// Any text is accepted and existing values are offered as suggestions; a typed
    /// value that already exists is flagged. Use it where a new id may be entered.
    /// </summary>
    Suggest,
}
