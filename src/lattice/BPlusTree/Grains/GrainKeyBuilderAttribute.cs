namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Marks a <see langword="static"/> method that composes an Orleans grain
/// <b>primary key</b> from its parts. The composed string becomes the grain
/// identity, which keyed storage backends carry into places that reject certain
/// characters: Azure Table grain storage puts the key into both the
/// Partition/Row key columns and the request URL, and those forbid the control
/// characters <c>0x00-0x1F</c> and <c>0x7F-0x9F</c> as well as
/// <c>/</c>, <c>\</c>, <c>#</c> and <c>?</c>. A composer that joins its parts
/// with such a character (for example an ASCII Unit Separator, <c>0x1F</c>)
/// yields a grain that cannot activate on that backend - an opaque HTTP 400
/// "Invalid URL" that no in-memory test storage reproduces.
/// <para>
/// The grain-key storage-safety guard
/// (<c>GrainKeyStorageSafetyContractTestsBase</c> in the shared testing library)
/// discovers every method bearing this attribute by reflection and asserts that
/// its output is free of those characters for representative inputs, so a newly
/// added composer is audited automatically rather than surfacing as a
/// production activation failure. Apply it to the static key-composition method
/// (never inline a compound grain key with a raw delimiter), and keep the
/// composed key deterministic and unambiguous.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
internal sealed class GrainKeyBuilderAttribute : Attribute;
