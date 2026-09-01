"""Phase 4 -- Medicare Advantage Plan Finder (MPF) element coverage + MA classifier,
plus the Plan-Net directory-extensions assessment.

CMS-4208-F2 (finalized under 42 CFR 422.111(m)) requires MA organizations to
make their provider/facility directory available in a CMS-specified digital,
machine-readable format. The CMS Technical Implementation Guide (dated Feb 18
2026) names **Plan-Net 1.2.0** as that FHIR format and enumerates the required
element list this module encodes. ``cms.gov`` 403s automated fetches; the guide
was read via a DocumentCloud mirror. That provenance gap is carried in
``MPF_SOURCE_PROVENANCE`` and MUST be surfaced wherever MPF numbers are
reported -- it is a real limitation on how authoritative this rule set is, not
an implementation detail to bury.

Two structural notes:

* HealthcareService and Endpoint carry NO checks here -- the guide's element
  list does not require either resource. Their absence from MPF_CHECKS is
  deliberate, not an oversight (see MPF_CALLOUTS).
* Telehealth availability, physical/communication accessibility, and
  cultural/linguistic capability are ALSO absent from the guide's element
  list, despite 42 CFR 422.111(b)(3)(i) requiring MA plans to disclose
  auxiliary aids/services and language assistance. That is a reportable gap in
  the MPF spec itself, not something this project can measure into existence
  -- flagged in MPF_CALLOUTS for the report layer (the accessibility /
  communication-proficiency / delivery-method extensions measured under
  ``assessment='plannet_ext'`` below are the closest a Plan-Net directory gets
  to covering it, and that assessment is explicitly labeled NOT USCDI v3 and
  NOT part of the MPF spec either).

MPF measurement applies ONLY to payers the classifier below identifies as
Medicare Advantage; Plan-Net-extensions coverage applies to EVERY payer (any
consumer-facing directory needs newpatients/communication/delivery/
accessibility signaling regardless of line of business).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import psycopg

from ..config import EndpointManifest
from ..etl.loader import list_payer_schemas, schema_for, set_search_path
from .element_coverage import CoverageCheck
from .uscdi import US_NPI_SYSTEM

MPF_SOURCE_PROVENANCE = {
    "document": "CMS Technical Implementation Guide for the MA/MAPD/PDP "
                 "Provider/Facility Directory Digital Format",
    "dated": "2026-02-18",
    "regulation": "42 CFR 422.111(m) (CMS-4208-F2)",
    "fhir_format": "Da Vinci PDex Plan-Net 1.2.0",
    "access_note": "cms.gov 403s automated fetches; read via a DocumentCloud "
                    "mirror of the published PDF. Element list transcribed "
                    "from that mirror, not cross-checked against a second "
                    "independent source.",
}

MPF_CALLOUTS = [
    "HealthcareService and Endpoint are NOT in the guide's required element "
    "list; MPF_CHECKS deliberately carries no checks for either resource.",
    "Telehealth availability, accessibility, and cultural/linguistic "
    "capability are absent from the MPF element list despite 42 CFR "
    "422.111(b)(3)(i) requiring MA plans to disclose auxiliary aids/services "
    "and language assistance -- a gap in the CMS guide, not in this "
    "measurement. The plannet_ext assessment's accessibility/"
    "communication-proficiency/delivery-method checks are the closest "
    "available signal and are NOT part of the MPF spec.",
]

NUCC_SYSTEM = "http://nucc.org/provider-taxonomy"
# Verified against a real sample: '207Q00000X' = 9 chars of [0-9A-Z] + trailing X.
NUCC_FORMAT_RE_SQL = "^[0-9A-Z]{9}X$"
# CMS MA/Part D contract-id family: H|R|S|E + 4-digit contract number, optionally
# + 3-digit PBP + 4-digit segment. Verified against devoted 'H0413-003-000',
# humana 'H5525-049-000-2021'; kaiser's XV-prefixed and fallon's bare
# 2-digit ids correctly fail.
MA_PLAN_ID_FORMAT_RE_SQL = "^[HRSE][0-9]{4}[0-9]{3}([0-9]{3})?([0-9]{4})?$"
MA_PLAN_ID_FORMAT_RE = re.compile(MA_PLAN_ID_FORMAT_RE_SQL)

PLANNET_EXT_BASE = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
NEWPATIENTS_URL = PLANNET_EXT_BASE + "newpatients"
COMMUNICATION_PROFICIENCY_URL = PLANNET_EXT_BASE + "communication-proficiency"
DELIVERY_METHOD_URL = PLANNET_EXT_BASE + "delivery-method"
ACCESSIBILITY_URL = PLANNET_EXT_BASE + "accessibility"

PLANNET_EXT_ATTRIBUTION = "Plan-Net 1.1 IG extensions -- NOT USCDI v3"
# Extension `context` in the IG package is generically `Element` for all four;
# the ACTUAL resource-type scoping (verified 2026-08 against
# F:/fhir_cache/packages/hl7.fhir.us.davinci-pdex-plan-net#1.1.0/package,
# grepping which plannet-<Resource> profiles reference each extension url) is:
#   newpatients                -> HealthcareService, Location, PractitionerRole
#   communication-proficiency  -> Practitioner
#   delivery-method            -> HealthcareService (carries a nested
#                                  virtualModalities sub-extension)
#   accessibility               -> Location
# This does NOT match the plan doc's initial guess (newpatients+delivery-method
# on PractitionerRole, accessibility on Practitioner-or-Location) -- the IG
# profiles are authoritative over that guess.
PLANNET_EXT_CONTEXT = {
    "newpatients": ["HealthcareService", "Location", "PractitionerRole"],
    "communication-proficiency": ["Practitioner"],
    "delivery-method": ["HealthcareService"],
    "accessibility": ["Location"],
}


def _ext_present_sql(url: str) -> str:
    return f'jsonb_path_exists(resource, \'$.extension[*] ? (@.url == "{url}")\')'


def _ext_present_pred(url: str):
    return lambda r: any(e.get("url") == url for e in (r.get("extension") or []))


# --- MA classifier -----------------------------------------------------

@dataclass(frozen=True)
class MASignal:
    name: str
    sql: str          # SELECT DISTINCT <matched value> ... WHERE payer_id = %s ... LIMIT 5
    description: str


MA_SIGNALS: list[MASignal] = [
    MASignal(
        "organization_type_medicare_like",
        "SELECT DISTINCT c FROM organization, unnest(type_codes) c "
        "WHERE payer_id = %s AND lower(c) ~ '^medi' AND lower(c) !~ 'medicaid' LIMIT 5",
        "Organization.type code starting 'medi' (Medicare*), excluding Medicaid",
    ),
    MASignal(
        "insurance_plan_plan_type",
        "SELECT DISTINCT c FROM insurance_plan, unnest(plan_type_codes) c "
        "WHERE payer_id = %s AND upper(c) = ANY(ARRAY['MA','MA-PD','MAPD','MMP']) LIMIT 5",
        "InsurancePlan.plan.type code in {MA, MA-PD, MAPD, MMP}",
    ),
    MASignal(
        "insurance_plan_name",
        "SELECT DISTINCT name FROM insurance_plan WHERE payer_id = %s "
        "AND name !~* '(supplement|medigap|medicaid)' AND ("
        r"name ~* '(medicare\s*advantage|\bmapd\b|\bmmp\b|\b[cdi]-?snp\b|special\s*needs\s*plan)'"
        # Bare "<Brand> Medicare" (e.g. sentara's 'Optima Medicare',
        # capital_health_plan's plan literally named 'Medicare') is a real MA
        # signal too, common for small regional carriers whose sole Medicare
        # product isn't spelled "...Advantage..." -- but ONLY when it is not
        # standalone Part D (a separate CMS product line, not MA) UNLESS
        # 'advantage' also appears (a combined MAPD plan legitimately says both).
        # The carve-out must catch every way payers actually BRAND standalone
        # Part D, not only the literal phrase "Part D": review showed
        # 'XYZ Medicare PDP' and 'XYZ Medicare Prescription Drug Plan' slipping
        # through the original 'part d'-only exclusion and misclassifying a
        # PDP-only payer as MA.
        r" OR (name ~* '\ymedicare\y' AND (name ~* 'advantage'"
        r" OR name !~* '(\ypart\s*d\y|\ypdp\y|prescription\s*drug)'))"
        # A CMS MA/Part D contract id (H|R|S|E + 4 digits [+ -PBP]) embedded in
        # the plan's display name rather than a structured identifier --
        # next_blue_nd's 'BCBSND & Noridian Retirees Group 500 H6202-803 ...'.
        r" OR name ~* '[HRSE][0-9]{4}-?[0-9]{3}'"
        ") LIMIT 5",
        # Deliberately NO bare `\bma\b` token: Fallon (and any Massachusetts
        # payer) writes plan names like "... Plan (MA)" where "MA" is the
        # STATE abbreviation, not "Medicare Advantage" -- a bare token would
        # false-positive on every Massachusetts payer's whole catalog, MA and
        # non-MA plans alike. The phrases above catch real MA plans (including
        # SNP variants, bare-branded "Medicare" plans, and embedded contract
        # ids) without that trap.
        "InsurancePlan.name matches Medicare-Advantage-ish phrasing (explicit "
        "'advantage'/MAPD/MMP/SNP wording, OR bare 'Medicare' branding that "
        "isn't standalone Part D, OR an embedded CMS contract id), excluding "
        "supplement/medigap/medicaid (bare 'MA' token deliberately excluded: "
        "Fallon's '...(MA)' state-abbreviation trap)",
    ),
    MASignal(
        "network_name_medicare",
        "SELECT DISTINCT name FROM organization WHERE payer_id = %s "
        "AND is_network = 1 AND name !~* 'medicaid' AND ("
        # Same standalone-Part-D carve-out as insurance_plan_name above: AHF's
        # 'AHF02 MEDICARE PART D (RETAIL)' network is a standalone PDP, not MA.
        r"(name ~* 'medicare' AND (name ~* 'advantage'"
        r" OR name !~* '(\ypart\s*d\y|\ypdp\y|prescription\s*drug)'))"
        # C/D/I-SNP networks are Medicare Advantage special-needs plans even
        # when the network name never spells out "Medicare" or "Advantage" at
        # all -- jhhp_advantage_md's 'JOHNS HOPKINS ADVANTAGE MD HMO - DSNP'.
        r" OR name ~* '\y[cdi]-?snp\y'"
        ") LIMIT 5",
        "a network-type Organization named with Medicare(-Advantage) or a "
        "C/D/I-SNP marker, excluding standalone Part D and Medicaid",
    ),
    MASignal(
        "contract_id_hr_prefix",
        "SELECT DISTINCT regexp_replace(i->>'value', '[^A-Za-z0-9]', '', 'g') AS v "
        "FROM insurance_plan, jsonb_array_elements(resource->'identifier') i "
        "WHERE payer_id = %s "
        "AND regexp_replace(i->>'value', '[^A-Za-z0-9]', '', 'g') ~ '^[HR][0-9]{4}' LIMIT 5",
        "an InsurancePlan.identifier normalizing to an H/R-prefixed CMS contract id",
    ),
]

# Payers with real, well-documented MA business whose signals (as currently
# tuned) may under- or over-fire, plus the reasoning -- kept as an explicit,
# reviewable override rather than a regex tweak that fits one payer and drifts
# the classifier for everyone else.
MA_OVERRIDE_INCLUDE: dict[str, str] = {
    "devoted": "publicly MA-only carrier; plan catalog is C/D-SNP naming "
               "that may not trip every signal depending on how plan names "
               "are formatted in this capture",
    # Public-record overrides (user-approved 2026-08-10): payers that
    # demonstrably sell Medicare Advantage but whose CAPTURED directory data
    # carries no machine-readable MA marker (empty or unmarked insurance_plan
    # rows). The MPF submission requirement (42 CFR 422.111(m)) attaches to
    # the MA ORGANIZATION, not to whether this capture happens to include its
    # plan catalog -- so their directories are in scope. Evidence string
    # 'public-record override' distinguishes these from data-driven rows in
    # every report.
    "healthteam": "public-record override: HealthTeam Advantage is an "
                  "MA-focused carrier (NC)",
    "zing_health": "public-record override: Zing Health is MA-only",
    "ucare": "public-record override: UCare sells MA (major MN Medicare carrier)",
    "viva_health": "public-record override: VIVA Health sells VIVA Medicare (AL)",
    "network_health": "public-record override: Network Health sells MA (WI)",
    "alliant": "public-record override: Alliant Health Plans sells MA (GA)",
    "aultcare": "public-record override: AultCare's PrimeTime Health Plan is MA (OH)",
    "christus": "public-record override: CHRISTUS Health Plan sells MA (TX/NM/LA)",
    "florida_blue": "public-record override: Florida Blue sells BlueMedicare MA",
    "bcbs_ks": "public-record override: BCBS of Kansas sells MA",
    "hcsc": "public-record override: HCSC sells MA (and acquired Cigna's "
            "Medicare business, closed 2025)",
    "western_health": "public-record override: Western Health Advantage "
                      "sells MA (CA)",
    "bcbs_az_new": "public-record override: AZ Blue sells MA (BlueJourney / "
                   "Blue MA products); the captured directory carries "
                   "'MA BLUEVISTA PIMA HMO' / 'MA BLUEINSPIRE MARIOCPA PIMA "
                   "PPO' network names, which use the bare 'MA ' abbreviation "
                   "no signal regex matches",
}
MA_OVERRIDE_EXCLUDE: dict[str, str] = {}


def classify_ma(conn: psycopg.Connection, manifest: EndpointManifest) -> list[dict]:
    """Classify every KNOWN payer with loaded data as MA or not, via MA_SIGNALS.

    Runs over ALL known payers regardless of any --subset the coverage run
    itself is scoped to -- the roster is a standalone deliverable the plan
    calls for a checkpoint on, not just an input to the MPF sweep. Touches
    only organization/insurance_plan tables (small even for aetna_cvs), never
    practitioner_role, so it is safe to run unconditionally.
    """
    have_data = list_payer_schemas(conn)
    out = []
    for ep in manifest.known():
        schema = schema_for(ep.key)
        if schema not in have_data:
            continue
        set_search_path(conn, schema)
        evidence = []
        for sig in MA_SIGNALS:
            with conn.cursor() as cur:
                cur.execute(sig.sql, (ep.key,))
                matches = [r[0] for r in cur.fetchall()]
            if matches:
                evidence.append({"signal": sig.name, "description": sig.description, "matches": matches})
        signaled = bool(evidence)
        override = None
        if ep.key in MA_OVERRIDE_EXCLUDE:
            classified = False
            override = f"excluded: {MA_OVERRIDE_EXCLUDE[ep.key]}"
        elif ep.key in MA_OVERRIDE_INCLUDE:
            classified = True
            override = f"included: {MA_OVERRIDE_INCLUDE[ep.key]}"
        else:
            classified = signaled
        out.append({
            "payer": ep.key, "classified": classified,
            "evidence": evidence, "override": override,
        })
    return out


# --- MPF element checks (MA-classified payers only) ---------------------
MPF_CHECKS: list[CoverageCheck] = [
    # PractitionerRole
    CoverageCheck("npi_identifier", "PractitionerRole",
                  f"jsonb_path_exists(resource, '$.identifier[*] ? (@.system == \"{US_NPI_SYSTEM}\")')",
                  False,
                  lambda r: any(i.get("system") == US_NPI_SYSTEM and i.get("value")
                               for i in (r.get("identifier") or [])),
                  mapping="mpf"),
    CoverageCheck("last_updated", "PractitionerRole",
                  "meta_last_updated IS NOT NULL", True,
                  lambda r: bool((r.get("meta") or {}).get("lastUpdated")), mapping="mpf"),
    CoverageCheck("last_updated_within_30d_of_capture", "PractitionerRole",
                  "(meta_last_updated IS NOT NULL AND "
                  "meta_last_updated >= (%(asof)s::timestamptz - interval '30 days'))",
                  True, None,
                  required=False, mapping="mpf",
                  note="staleness at capture -- not the regulatory MPF freshness metric"),
    CoverageCheck("telecom_phone", "PractitionerRole",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="mpf"),
    CoverageCheck("specialty_presence", "PractitionerRole",
                  "coalesce(cardinality(specialty_codes), 0) > 0", True,
                  lambda r: bool(r.get("specialty")), mapping="mpf"),
    CoverageCheck("specialty_nucc_format", "PractitionerRole",
                  f"EXISTS (SELECT 1 FROM unnest(specialty_codes) c WHERE c ~ '{NUCC_FORMAT_RE_SQL}')",
                  True, None,
                  required=False, mapping="mpf", note="NUCC taxonomy code format tier"),
    # jsonb_path_exists (lax), not jsonb_array_elements: the latter raises on
    # a non-array value and one malformed row would abort the payer's merged
    # scan; lax jsonpath treats structural mismatches as "no match".
    CoverageCheck("specialty_system_tier", "PractitionerRole",
                  "jsonb_path_exists(resource, '$.specialty[*].coding[*]"
                  f" ? (@.system == \"{NUCC_SYSTEM}\")')",
                  False,
                  lambda r: any(cod.get("system") == NUCC_SYSTEM
                               for s in (r.get("specialty") or []) for cod in (s.get("coding") or [])),
                  required=False, mapping="mpf"),
    CoverageCheck("network_refs", "PractitionerRole",
                  "coalesce(cardinality(network_refs), 0) > 0", True,
                  # Mirrors transform._network_refs: Plan-Net carries network
                  # membership as an extension (url containing "network")
                  # whose value is a Reference, not a top-level field.
                  lambda r: any("network" in (e.get("url") or "").lower() and e.get("valueReference")
                               for e in (r.get("extension") or [])),
                  mapping="mpf"),
    CoverageCheck("location_refs", "PractitionerRole",
                  "coalesce(cardinality(location_refs), 0) > 0", True,
                  lambda r: bool(r.get("location")), mapping="mpf"),
    CoverageCheck("accepting_new_patients", "PractitionerRole",
                  _ext_present_sql(NEWPATIENTS_URL), False, _ext_present_pred(NEWPATIENTS_URL),
                  required=False, mapping="mpf",
                  note="optional per the guide; same field measured again under "
                       "assessment='plannet_ext'"),

    # Practitioner
    CoverageCheck("npi_identifier_practitioner_level", "Practitioner",
                  "npi IS NOT NULL", True, None,
                  required=False, mapping="mpf",
                  note="companion context for PractitionerRole npi_identifier: the "
                       "guide puts NPI at the PractitionerRole level, but payers "
                       "commonly publish it on Practitioner instead (probed live: "
                       "humana 0% at PR level / 100% here; devoted 100% at both). "
                       "Without this row a PR-level 0% reads as 'publishes no NPI' "
                       "when the NPI is one reference hop away."),
    CoverageCheck("family", "Practitioner", "family IS NOT NULL", True,
                  lambda r: bool((r.get("name") or [{}])[0].get("family")), mapping="mpf"),
    CoverageCheck("given", "Practitioner", "given IS NOT NULL", True,
                  lambda r: bool((r.get("name") or [{}])[0].get("given")), mapping="mpf"),
    CoverageCheck("communication", "Practitioner", "(resource ? 'communication')", False,
                  lambda r: "communication" in r, mapping="mpf"),
    CoverageCheck("gender", "Practitioner", "(resource ? 'gender')", False,
                  lambda r: "gender" in r, required=False, mapping="mpf"),

    # Location
    CoverageCheck("address_line", "Location", "address_line IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("address_city", "Location", "address_city IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("address_state", "Location", "address_state IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("address_postalcode", "Location", "address_postalcode IS NOT NULL", True, None,
                  mapping="mpf"),
    CoverageCheck("address_complete", "Location",
                  "(address_line IS NOT NULL AND address_city IS NOT NULL "
                  "AND address_state IS NOT NULL AND address_postalcode IS NOT NULL)",
                  True, None, mapping="mpf"),
    CoverageCheck("phone", "Location",
                  "jsonb_path_exists(resource, '$.telecom[*] ? (@.system == \"phone\")')",
                  False,
                  lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])),
                  mapping="mpf"),

    # Organization -- facilities cell only (networks are a separate directory concept)
    CoverageCheck("npi", "Organization", "npi IS NOT NULL", True, None,
                  mapping="mpf", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("name", "Organization", "name IS NOT NULL", True, None,
                  mapping="mpf", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("last_updated", "Organization", "meta_last_updated IS NOT NULL", True, None,
                  mapping="mpf", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("type_codes", "Organization", "coalesce(cardinality(type_codes), 0) > 0", True, None,
                  mapping="mpf", cell_where="coalesce(is_network, 0) = 0"),
    CoverageCheck("type_nucc_system", "Organization",
                  "jsonb_path_exists(resource, '$.type[*].coding[*]"
                  f" ? (@.system == \"{NUCC_SYSTEM}\")')",
                  False,
                  lambda r: any(cod.get("system") == NUCC_SYSTEM
                               for t in (r.get("type") or []) for cod in (t.get("coding") or [])),
                  required=False, mapping="mpf",
                  cell_where="coalesce(is_network, 0) = 0",
                  note="Organization.type is formally bound to OrgTypeCS (extensible), "
                       "not NUCC; this checks for the NUCC provider-taxonomy system "
                       "specifically as an informational curiosity, since some payers "
                       "tag facility orgs with taxonomy codes anyway"),
    CoverageCheck("address", "Organization", "(resource ? 'address')", False,
                  lambda r: "address" in r, mapping="mpf",
                  cell_where="coalesce(is_network, 0) = 0"),

    # OrganizationAffiliation
    CoverageCheck("network_refs", "OrganizationAffiliation",
                  "coalesce(cardinality(network_refs), 0) > 0", True, None, mapping="mpf"),
    CoverageCheck("organization_ref", "OrganizationAffiliation",
                  "organization_ref IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("participating_organization_ref", "OrganizationAffiliation",
                  "participating_organization_ref IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("location_refs", "OrganizationAffiliation",
                  "coalesce(cardinality(location_refs), 0) > 0", True, None, mapping="mpf"),
    CoverageCheck("specialty_codes", "OrganizationAffiliation",
                  "coalesce(cardinality(specialty_codes), 0) > 0", True, None, mapping="mpf"),

    # InsurancePlan
    CoverageCheck("name", "InsurancePlan", "name IS NOT NULL", True, None, mapping="mpf"),
    CoverageCheck("identifier", "InsurancePlan", "(resource ? 'identifier')", False,
                  lambda r: "identifier" in r, mapping="mpf"),
    CoverageCheck("network_refs", "InsurancePlan",
                  "coalesce(cardinality(network_refs), 0) > 0", True, None, mapping="mpf"),
    CoverageCheck("period_start", "InsurancePlan",
                  "(resource #>> '{period,start}') IS NOT NULL", False,
                  lambda r: bool((r.get("period") or {}).get("start")), mapping="mpf"),
    CoverageCheck("ma_plan_id_format", "InsurancePlan",
                  "(jsonb_typeof(resource->'identifier') = 'array' AND EXISTS ("
                  "SELECT 1 FROM jsonb_array_elements(resource->'identifier') i "
                  "WHERE regexp_replace(i->>'value', '[^A-Za-z0-9]', '', 'g') "
                  f"~ '{MA_PLAN_ID_FORMAT_RE_SQL}'))",
                  False,
                  lambda r: any(
                      MA_PLAN_ID_FORMAT_RE.match(re.sub(r"[^A-Za-z0-9]", "", i.get("value") or ""))
                      for i in (r.get("identifier") or [])
                  ) if isinstance(r.get("identifier"), list) else False,
                  required=False, mapping="mpf",
                  note="format/heuristic tier, like the NUCC-format checks above: real "
                       "CMS H/R/S/E contract-id shape, verified against devoted "
                       "('H0413-003-000' -> match), humana ('H5525-049-000-2021' -> "
                       "match); kaiser's XV-prefixed and fallon's bare 2-digit ids "
                       "correctly fail"),
]


# --- Plan-Net directory-extensions coverage (ALL payers) -----------------
PLANNET_EXT_CHECKS: list[CoverageCheck] = [
    *[
        CoverageCheck("newpatients", rtype, _ext_present_sql(NEWPATIENTS_URL), False,
                      _ext_present_pred(NEWPATIENTS_URL), mapping="extension")
        for rtype in PLANNET_EXT_CONTEXT["newpatients"]
    ],
    CoverageCheck("communication_proficiency", "Practitioner",
                  _ext_present_sql(COMMUNICATION_PROFICIENCY_URL), False,
                  _ext_present_pred(COMMUNICATION_PROFICIENCY_URL), mapping="extension"),
    CoverageCheck("delivery_method", "HealthcareService",
                  _ext_present_sql(DELIVERY_METHOD_URL), False,
                  _ext_present_pred(DELIVERY_METHOD_URL), mapping="extension"),
    CoverageCheck("delivery_method_virtual_modalities", "HealthcareService",
                  # lax jsonpath instead of nested jsonb_array_elements: see
                  # specialty_system_tier's WHY-comment in MPF_CHECKS.
                  f"jsonb_path_exists(resource, '$.extension[*]"
                  f" ? (@.url == \"{DELIVERY_METHOD_URL}\")"
                  ".extension[*] ? (@.url == \"virtualModalities\")')",
                  False,
                  lambda r: any(
                      e.get("url") == DELIVERY_METHOD_URL
                      and any(s.get("url") == "virtualModalities" for s in (e.get("extension") or []))
                      for e in (r.get("extension") or [])
                  ),
                  required=False, mapping="extension",
                  note="sub-extension of delivery-method; informational since a payer "
                       "may legitimately have no virtual-delivery services at all"),
    CoverageCheck("accessibility", "Location",
                  _ext_present_sql(ACCESSIBILITY_URL), False,
                  _ext_present_pred(ACCESSIBILITY_URL), mapping="extension"),
]
