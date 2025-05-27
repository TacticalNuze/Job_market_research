#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import pandas as pd
from collections import defaultdict

def build_mcd(offers):
    offers_table = []
    profiles_table = {}
    locations_table = {}
    salaries_table = {}
    soft_skills_table = {}
    hard_skills_table = {}
    offer_soft_skills = []
    offer_hard_skills = []

    # Index counters
    profile_idx = location_idx = salary_idx = skill_idx = 1
    soft_skill_ids = {}
    hard_skill_ids = {}

    for i, offer in enumerate(offers):
        if offer.get("is_data_profile") != 1:
            continue  # Skip non-data profiles

        offer_id = i + 1  # Primary key

        # --- Profile ---
        profile = offer.get("profile", "unknown").strip().lower()
        if profile not in profiles_table:
            profiles_table[profile] = profile_idx
            profile_idx += 1
        profile_id = profiles_table[profile]

        # --- Location ---
        loc = offer.get("location")
        if isinstance(loc, dict):
            loc_str = ", ".join(filter(None, [
                loc.get("ville", ""), loc.get("region", ""), loc.get("pays", "")
            ])).strip().lower()
        else:
            loc_str = str(loc or "unknown").strip().lower()

        if loc_str not in locations_table:
            locations_table[loc_str] = location_idx
            location_idx += 1
        location_id = locations_table[loc_str]

        # --- Salary ---
        salaire = offer.get("salary_range") or "unknown"
        if isinstance(salaire, dict):
            salaire_str = f"{salaire.get('min', '')}-{salaire.get('max', '')} {salaire.get('devise', '')}".strip()
        else:
            salaire_str = str(salaire).strip()
        if salaire_str not in salaries_table:
            salaries_table[salaire_str] = salary_idx
            salary_idx += 1
        salary_id = salaries_table[salaire_str]

        # --- Main Offer Entry ---
        offers_table.append({
            "offer_id": offer_id,
            "job_url": offer.get("job_url", ""),
            "titre": offer.get("titre", ""),
            "via": offer.get("via", ""),
            "publication_date": offer.get("publication_date", ""),
            "education_level": offer.get("education_level", None),
            "experience_years": offer.get("experience_years", None),
            "seniority": offer.get("seniority", ""),
            "profile_id": profile_id,
            "location_id": location_id,
            "salary_id": salary_id
        })

        # --- Soft Skills ---
        for skill in offer.get("soft_skills") or []:
            skill = skill.strip().lower()
            if skill not in soft_skill_ids:
                soft_skill_ids[skill] = skill_idx
                skill_idx += 1
            offer_soft_skills.append({
                "offer_id": offer_id,
                "skill_id": soft_skill_ids[skill]
            })

        # --- Hard Skills ---
        for skill in offer.get("hard_skills") or []:
            skill = skill.strip().lower()
            if skill not in hard_skill_ids:
                hard_skill_ids[skill] = skill_idx
                skill_idx += 1
            offer_hard_skills.append({
                "offer_id": offer_id,
                "skill_id": hard_skill_ids[skill]
            })

    return {
        "offers": offers_table,
        "profiles": [{"profile_id": pid, "profile": prof} for prof, pid in profiles_table.items()],
        "locations": [{"location_id": lid, "location": loc} for loc, lid in locations_table.items()],
        "salaries": [{"salary_id": sid, "salary_range": sal} for sal, sid in salaries_table.items()],
        "soft_skills": [{"skill_id": sid, "skill": s} for s, sid in soft_skill_ids.items()],
        "hard_skills": [{"skill_id": sid, "skill": s} for s, sid in hard_skill_ids.items()],
        "offer_soft_skills": offer_soft_skills,
        "offer_hard_skills": offer_hard_skills
    }

def save_to_excel(tables, filename):
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for table_name, records in tables.items():
            pd.DataFrame(records).to_excel(writer, sheet_name=table_name[:31], index=False)

def main():
    if len(sys.argv) != 2:
        print("Usage: python build_mcd.py <enriched_json>")
        sys.exit(1)

    file_path = sys.argv[1]
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    tables = build_mcd(data)
    save_to_excel(tables, "mcd_output.xlsx")
    print("✅ Exported successfully to mcd_output.xlsx")

if __name__ == "__main__":
    main()
