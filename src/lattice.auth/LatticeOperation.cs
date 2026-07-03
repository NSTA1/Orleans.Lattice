namespace Orleans.Lattice.Auth;

/// <summary>
/// The set of operations an authorization rule covers, modelled as a bit flag so
/// a single rule can grant or deny several operations at once (for example
/// <c>Read | Enumerate</c>).
/// <para>
/// <b>Cross-issue note:</b> this is a minimal, local placeholder for the core
/// <c>Orleans.Lattice.LatticeOperation</c> flags enum being introduced on a
/// parallel branch. It is defined here only so the rule model compiles ahead of
/// that core type landing; once the core enum is available this local copy is
/// intended to be removed and the rule's operations field retargeted at the core
/// type. Keep the flag names and bit layout reconcilable with the core enum.
/// </para>
/// </summary>
[Flags]
public enum LatticeOperation
{
    /// <summary>No operation.</summary>
    None = 0,

    /// <summary>Point reads of a single key's current value.</summary>
    Read = 1 << 0,

    /// <summary>Writes that create or replace a key's value.</summary>
    Write = 1 << 1,

    /// <summary>Deletes that remove a key.</summary>
    Delete = 1 << 2,

    /// <summary>Range scans over keys or entries.</summary>
    Enumerate = 1 << 3,

    /// <summary>Administrative operations against a tree (resize, reshard, retention, and similar).</summary>
    Administer = 1 << 4,

    /// <summary>Every defined operation.</summary>
    All = Read | Write | Delete | Enumerate | Administer,
}
