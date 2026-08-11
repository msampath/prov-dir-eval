"""Phase 4 -- USCDI v3 element coverage rule set.

USCDI (United States Core Data for Interoperability) is ONC's federally-defined
minimum data set for health IT interoperability -- the current baseline
(effective certification requirement 45 CFR 170.213, HTI-1) is **USCDI v3.1**
(published June 2025; certification deadline January 1, 2026). USCDI is
resource-agnostic: it defines *data classes and elements*, not FHIR profiles.

There is NO official USCDI -> Plan-Net element mapping. US Core (which does
carry the authoritative USCDI-to-FHIR bindings for clinical data) maps USCDI's
"Care Team Member" class to the ``CareTeam`` resource only -- Plan-Net has no
CareTeam resource at all. Every element below is therefore an INFERENCE: we are
answering "if a Plan-Net directory were asked to carry the closest analog of
this USCDI element, would it be populated", not "does this satisfy the USCDI
requirement" (which requires a CareTeam resource this IG does not define).
Each check's ``mapping`` field says which kind of inference it is:

  ``inferred``  -- element maps onto a natural Plan-Net analog (Practitioner as
                   proxy for CareTeam Member, e.g. name / identifier / role).
  ``analog``    -- looser: a Plan-Net field that serves a similar PURPOSE to
                   the USCDI element (e.g. Location.address for the
                   patient-demographics "address" element) but is not the same
                   subject at all (a facility's address, not a person's).
  ``proxy``     -- stands in for a USCDI element we cannot observe directly
                   (meta.lastUpdated as a proxy for Provenance's Author Time
                   Stamp -- it records "when this record last changed", not
                   "when this specific fact was attested").
  ``weak-proxy``-- same idea as `proxy` but too loose to trust as a headline
                   number; included for completeness, excluded from the
                   headline mean by construction (see uscdi_headline below).
  ``unmeasurable`` -- no SQL and no on-disk sample can answer this at all
                   (Provenance.Author Organization; Group.Identifier -- Group
                   is not a resource this project loads). Emitted as an
                   explicit gap row (denominator 0) rather than omitted, so a
                   USCDI element list reader sees "we looked, and could not
                   measure this" instead of a silent absence.

USCDI v3 explicitly has **no Facility class** -- that arrives in USCDI v4+.
Specialty, network membership, and accepting-patients status are likewise
absent from USCDI v3's element list; those live in the MPF assessment
(mpf.py) and the Plan-Net-extensions assessment, not here. Reporting code
(scripts/element_coverage_report.py) must carry this "Facility is v4, not v3"
note verbatim alongside any USCDI coverage table.

Headline = the unweighted mean of MEASURABLE presence-tier rows whose mapping
is in ('inferred', 'analog', 'proxy') -- format/currency informational tiers
(required=False) and unmeasurable/weak-proxy rows are excluded by construction
(see uscdi_headline). This is a per-payer, per-element average, not weighted by
row count -- USCDI coverage asks "how many of the elements we can measure are
populated", not "how many rows have this element" (the latter is what
evaluate.py's completeness score already answers elsewhere).
"""

from __future__ import annotations

import re

from .element_coverage import CoverageCheck

US_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
_PHONE_FORMAT_RE = re.compile(r"^\+?[0-9()\-.\s]{6,19}$")
_RFC5646_STRICT_RE = re.compile(r"^[a-zA-Z]{2,3}(-[A-Za-z0-9]{2,8})*$")

# --- element metadata (USCDI v3.1 class/element names + source standard) ---
USCDI_ELEMENT_META: dict[str, dict] = {
    "care_team_member_name_family": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Name",
        "standard": "USCDI v3.1", "path": "Practitioner.name.family",
    },
    "care_team_member_name_given": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Name",
        "standard": "USCDI v3.1", "path": "Practitioner.name.given",
    },
    "care_team_member_identifier_npi": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Identifier",
        "standard": "USCDI v3.1", "path": "Practitioner.identifier (NPI)",
    },
    "care_team_member_role": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Role",
        "standard": "USCDI v3.1", "path": "PractitionerRole.code",
    },
    "care_team_member_location": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Location Reference",
        "standard": "USCDI v3.1", "path": "PractitionerRole.location",
    },
    "care_team_member_telecom_phone": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Telecom",
        "standard": "USCDI v3.1", "path": "Practitioner.telecom[system=phone]",
    },
    "care_team_member_telecom_phone_format": {
        "uscdi_class": "Care Team Member", "uscdi_element": "Telecom (format)",
        "standard": "USCDI v3.1 (informational)",
        "path": "Practitioner.telecom.value ~ E.123/E.164-plausible",
    },
    "demographics_location_address_complete": {
        "uscdi_class": "Patient Demographics/Address (analog)", "uscdi_element": "Address",
        "standard": "USCDI v3.1", "path": "Location.address (line+city+state+postalCode)",
    },
    "demographics_phone": {
        "uscdi_class": "Patient Demographics/Phone (analog)", "uscdi_element": "Phone Number",
        "standard": "USCDI v3.1", "path": "*.telecom[system=phone]",
    },
    "demographics_phone_type": {
        "uscdi_class": "Patient Demographics/Phone (analog)", "uscdi_element": "Phone Number Type",
        "standard": "USCDI v3.1 (informational)", "path": "*.telecom[system=phone].use",
    },
    "demographics_email": {
        "uscdi_class": "Patient Demographics/Email (analog)", "uscdi_element": "Email Address",
        "standard": "USCDI v3.1", "path": "*.telecom[system=email]",
    },
    "demographics_communication_presence": {
        "uscdi_class": "Patient Demographics/Language (analog)", "uscdi_element": "Preferred Language",
        "standard": "USCDI v3.1", "path": "Practitioner.communication",
    },
    "demographics_communication_rfc5646": {
        "uscdi_class": "Patient Demographics/Language (analog)", "uscdi_element": "Preferred Language (format)",
        "standard": "USCDI v3.1 (informational)",
        "path": "Practitioner.communication.coding.code ~ RFC-5646/BCP-47 strict (R4 shape)",
    },
    "provenance_author_timestamp": {
        "uscdi_class": "Provenance", "uscdi_element": "Author Time Stamp (proxy)",
        "standard": "USCDI v3.1", "path": "*.meta.lastUpdated",
    },
    "provenance_author_organization": {
        "uscdi_class": "Provenance", "uscdi_element": "Author Organization",
        "standard": "USCDI v3.1", "path": "(unmeasurable: no Provenance resource loaded)",
    },
    "group_identifier": {
        "uscdi_class": "Facility (n/a) / Group", "uscdi_element": "Group Identifier",
        "standard": "USCDI v3.1", "path": "(unmeasurable: no Group resource loaded)",
    },
    "insurance_plan_identifier": {
        "uscdi_class": "Health Insurance Information (weak proxy)", "uscdi_element": "Coverage Identifier",
        "standard": "USCDI v3.1 (informational; excluded from headline)",
        "path": "InsurancePlan.identifier",
    },
}

_HEADLINE_MAPPINGS = {"inferred", "analog", "proxy"}


def uscdi_headline(rows: list[dict]) -> float | None:
    """Unweighted mean pct across measurable presence-tier rows.

    "Measurable" = denominator > 0 (method != 'unmeasured'); "presence-tier" =
    required=True (the format/currency informational tiers are excluded
    explicitly, not just by having a smaller denominator); mapping restricted
    to the three headline-eligible labels, which also mechanically drops
    'weak-proxy' and 'unmeasurable' rows without a separate branch.
    """
    pcts = [
        100.0 * r["populated"] / r["denominator"]
        for r in rows
        if r["denominator"] > 0 and r["required"] and r["mapping"] in _HEADLINE_MAPPINGS
    ]
    return round(sum(pcts) / len(pcts), 1) if pcts else None


# --- checks ------------------------------------------------------------
USCDI_CHECKS: list[CoverageCheck] = [
    # Care Team Member (inferred from Practitioner / PractitionerRole)
    CoverageCheck("care_team_member_name_family", "Practitioner",
                  "family IS NOT NULL", True, lambda r: bool((r.get("name") or [{}])[0].get("family")),
                  mapping="inferred"),
    CoverageCheck("care_team_member_name_given", "Practitioner",
                  "given IS NOT NULL", True,
                  lambda r: bool((r.get("name") or [{}])[0].get("given")),
                  mapping="inferred"),
    CoverageCheck("care_team_member_identifier_npi", "Practitioner",
                  "npi IS NOT NULL", True,
                  lambda r: any(i.get("system") == US_NPI_SYSTEM and i.get("value")
                               for i in (r.get("identifier") or [])),
                  mapping="inferred"),
    CoverageCheck("care_team_member_role", "PractitionerRole",
                  "coalesce(cardinality(role_codes), 0) > 0", True,
                  lambda r: bool(r.get("code")),
                  mapping="inferred"),
    CoverageCheck("care_team_member_location", "PractitionerRole",
                  "coalesce(cardinality(location_refs), 0) > 0", True,
                  lambda r: bool(r.get("location")),
                  mapping="inferred"),
    CoverageCheck("care_team_member_telecom_phone", "Practitioner",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="inferred"),
    # jsonb_path_exists (lax mode), not jsonb_array_elements: the latter RAISES
    # on a row whose element is an object/scalar instead of an array, and one
    # malformed row would abort the payer's whole merged scan hours into a
    # sweep. Lax jsonpath treats structural mismatches as "no match".
    CoverageCheck("care_team_member_telecom_phone_format", "Practitioner",
                  r"""jsonb_path_exists(resource, '$.telecom[*] ? (@.system == "phone" """
                  r"""&& @.value like_regex "^\\+?[0-9()\\-.\\s]{6,19}$")')""",
                  False,
                  lambda r: any(
                      t.get("system") == "phone" and t.get("value")
                      and _PHONE_FORMAT_RE.match(t["value"])
                      for t in (r.get("telecom") or [])
                  ),
                  required=False, mapping="analog",
                  note="E.123/E.164-plausible informational tier, not a strict validator"),

    # Demographics analogs (Location/Organization address+telecom; Practitioner language)
    CoverageCheck("demographics_location_address_complete", "Location",
                  "(address_line IS NOT NULL AND address_city IS NOT NULL "
                  "AND address_state IS NOT NULL AND address_postalcode IS NOT NULL)",
                  True, None, mapping="analog"),
    CoverageCheck("demographics_phone", "Location",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="analog"),
    CoverageCheck("demographics_phone_type", "Location",
                  "jsonb_path_exists(resource, '$.telecom[*]"
                  " ? (@.system == \"phone\" && exists (@.use))')",
                  False,
                  lambda r: any(t.get("system") == "phone" and "use" in t for t in (r.get("telecom") or [])),
                  required=False, mapping="analog", note="phone use/type tagging (informational)"),
    CoverageCheck("demographics_email", "Location",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"email\")')",
                  False,
                  lambda r: any(t.get("system") == "email" for t in (r.get("telecom") or [])),
                  mapping="analog"),
    # Facilities only (is_network=0), matching MPF's Organization checks: a
    # plannet-Network is an administrative grouping entity, not a place a
    # member can phone or email, and mixing networks into the denominator
    # shifts these rates by however (un)populated network-org telecom happens
    # to be for each payer.
    CoverageCheck("demographics_phone", "Organization",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="analog", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("demographics_phone_type", "Organization",
                  "jsonb_path_exists(resource, '$.telecom[*]"
                  " ? (@.system == \"phone\" && exists (@.use))')",
                  False,
                  lambda r: any(t.get("system") == "phone" and "use" in t for t in (r.get("telecom") or [])),
                  required=False, mapping="analog", note="phone use/type tagging (informational)",
                  cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("demographics_email", "Organization",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"email\")')",
                  False,
                  lambda r: any(t.get("system") == "email" for t in (r.get("telecom") or [])),
                  mapping="analog", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("demographics_phone", "Practitioner",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="analog",
                  note="same underlying field as care_team_member_telecom_phone, "
                       "reported again under the Demographics analog class on purpose"),
    CoverageCheck("demographics_phone_type", "Practitioner",
                  "jsonb_path_exists(resource, '$.telecom[*]"
                  " ? (@.system == \"phone\" && exists (@.use))')",
                  False,
                  lambda r: any(t.get("system") == "phone" and "use" in t for t in (r.get("telecom") or [])),
                  required=False, mapping="analog", note="phone use/type tagging (informational)"),
    CoverageCheck("demographics_email", "Practitioner",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"email\")')",
                  False,
                  lambda r: any(t.get("system") == "email" for t in (r.get("telecom") or [])),
                  mapping="analog"),
    CoverageCheck("demographics_communication_presence", "Practitioner",
                  "(resource ? 'communication')", False,
                  lambda r: "communication" in r, mapping="analog"),
    # R4 shape: Practitioner.communication is CodeableConcept[] -- the coding
    # array hangs DIRECTLY off each entry. The `.language.coding` path is R5's
    # BackboneElement shape and matches nothing in this (R4, Plan-Net) corpus:
    # probed live, devoted publishes {"coding":[{"code":"en","system":
    # "urn:ietf:bcp:47"}]} which the R5 path scored 0%.
    CoverageCheck("demographics_communication_rfc5646", "Practitioner",
                  "jsonb_path_exists(resource, '$.communication[*].coding[*]"
                  " ? (@.code like_regex \"^[a-zA-Z]{2,3}(-[A-Za-z0-9]{2,8})*$\")')",
                  False,
                  lambda r: any(
                      _RFC5646_STRICT_RE.match(cod.get("code") or "")
                      for comm in (r.get("communication") or [])
                      for cod in ((comm or {}).get("coding") or [])
                  ),
                  required=False, mapping="analog", note="RFC-5646/BCP-47 strict tier (informational)"),

    # Provenance Author Time Stamp proxy: meta_last_updated on every resource type
    *[
        CoverageCheck("provenance_author_timestamp", rtype,
                      "meta_last_updated IS NOT NULL", True,
                      lambda r: bool((r.get("meta") or {}).get("lastUpdated")),
                      mapping="proxy")
        for rtype in ["Organization", "Practitioner", "PractitionerRole", "Location",
                      "HealthcareService", "OrganizationAffiliation", "InsurancePlan", "Endpoint"]
    ],

    # Explicit unmeasurable gap rows -- no SQL, no sample, can reach these.
    CoverageCheck("provenance_author_organization", "Provenance", None, False, None,
                  mapping="unmeasurable",
                  note="no Provenance resource is loaded by this project's ETL"),
    CoverageCheck("group_identifier", "Group", None, False, None,
                  mapping="unmeasurable",
                  note="no Group resource is loaded; Plan-Net has no Group profile"),

    # Weak-proxy, deliberately excluded from the headline via its mapping label.
    CoverageCheck("insurance_plan_identifier", "InsurancePlan",
                  "(resource ? 'identifier')", False,
                  lambda r: "identifier" in r,
                  required=False, mapping="weak-proxy",
                  note="loose proxy for a coverage/plan identifier; not trustworthy as a headline figure"),
]
