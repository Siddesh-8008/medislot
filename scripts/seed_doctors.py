"""Create 15 demo doctors through the deployed MediSlot API, each with consultation types and weekly hours.

Everything goes through the real endpoints, exactly like a doctor using the dashboard:
    POST /auth/register/doctor  ->  POST /auth/login  ->  POST /doctors/me/consultation-types
                                                       ->  POST /doctors/me/availability-patterns
so passwords are hashed by the authentication Lambda and the slot materialiser builds the bookable slots
automatically after each weekly pattern is added (give it a minute).

Safe to run again: a doctor that already exists is signed in instead of re-created, and consultation types or
weekly patterns that are already there are skipped. Standard library only.

    python scripts/seed_doctors.py --api https://<id>.execute-api.ap-south-1.amazonaws.com/dev
    python scripts/seed_doctors.py --dry-run            # just print the doctors, types, hours and passwords

Run `rds_check` first (it creates the hospitals, specializations and the four consultation types used here).
"""
import argparse
import json
import sys
import urllib.error
import urllib.request

# Reference data seeded by lambdas/rds_check (ids are fixed there).
HOSPITALS = {1: "Apollo Hospitals Jubilee Hills", 2: "Yashoda Hospitals Somajiguda", 3: "KIMS Hospitals Secunderabad",
             4: "CARE Hospitals Banjara Hills", 5: "AIG Hospitals Gachibowli", 6: "Continental Hospitals",
             7: "Rainbow Children's Hospital Banjara Hills", 8: "NIMS Hyderabad", 9: "Osmania General Hospital",
             10: "Gandhi Hospital"}
SPECIALIZATIONS = {1: "Cardiology", 2: "Dermatology", 3: "Endocrinology", 4: "Family Medicine", 5: "Gastroenterology",
                   6: "General Medicine", 9: "Gynecology", 11: "Nephrology", 12: "Neurology", 15: "Ophthalmology",
                   16: "Orthopedics", 17: "Otolaryngology (ENT)", 18: "Pediatrics", 19: "Psychiatry", 20: "Pulmonology"}
# consultation_type_id -> (name, minutes, min capacity, max capacity)
TYPES = {1: ("Quick Consult", 15, 1, 1), 2: ("Standard Consult", 30, 1, 1), 3: ("Detailed Consult", 60, 1, 1),
         4: ("Health Camp Session", 60, 2, 10)}
DAYS = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
Q, S, D, H = 1, 2, 3, 4

# first, last, hospital, specialization, years, languages, qualifications,
#   types:    [(type id, fee in rupees, capacity)],
#   patterns: [(type id, weekday 1=Mon..7=Sun, start, end)]   (no two patterns overlap on the same weekday)
DOCTORS = [
    ("Arjun", "Reddy", 1, 1, 14, "English, Telugu, Hindi", "MBBS, MD (Internal Medicine), DM (Cardiology)",
     [(S, 800, 1), (D, 1500, 1)],
     [(S, 1, "09:00", "12:00"), (D, 1, "14:00", "16:00"), (S, 3, "09:00", "12:00"), (D, 5, "10:00", "13:00")]),
    ("Priya", "Sharma", 2, 2, 9, "English, Hindi", "MBBS, MD (Dermatology)",
     [(Q, 400, 1), (S, 700, 1)],
     [(Q, 2, "10:00", "13:00"), (S, 2, "15:00", "18:00"), (Q, 4, "10:00", "13:00"), (S, 6, "09:00", "12:00")]),
    ("Kiran", "Rao", 3, 16, 18, "English, Telugu, Kannada", "MBBS, MS (Orthopaedics)",
     [(S, 900, 1), (D, 1600, 1)],
     [(S, 1, "10:00", "13:00"), (D, 3, "14:00", "17:00"), (S, 6, "09:00", "12:00")]),
    ("Ananya", "Iyer", 7, 18, 11, "English, Tamil, Telugu", "MBBS, MD (Paediatrics)",
     [(Q, 350, 1), (S, 600, 1), (H, 250, 8)],
     [(Q, 1, "09:00", "11:00"), (S, 1, "11:00", "13:00"), (S, 4, "09:00", "12:00"), (H, 7, "10:00", "13:00")]),
    ("Rahul", "Verma", 5, 5, 13, "English, Hindi", "MBBS, MD, DM (Gastroenterology)",
     [(S, 1000, 1), (D, 1800, 1)],
     [(S, 2, "09:00", "12:00"), (D, 4, "14:00", "17:00"), (S, 5, "09:00", "12:00")]),
    ("Sneha", "Patel", 4, 9, 10, "English, Hindi, Gujarati", "MBBS, MS (Obstetrics and Gynaecology)",
     [(S, 700, 1), (D, 1300, 1)],
     [(S, 1, "15:00", "18:00"), (S, 3, "10:00", "13:00"), (D, 6, "10:00", "12:00")]),
    ("Vikram", "Singh", 6, 12, 20, "English, Hindi, Punjabi", "MBBS, MD (Medicine), DM (Neurology)",
     [(D, 2000, 1), (S, 1200, 1)],
     [(D, 2, "10:00", "13:00"), (S, 4, "10:00", "12:00"), (D, 5, "14:00", "17:00")]),
    ("Lakshmi", "Nair", 8, 6, 7, "English, Malayalam, Telugu", "MBBS, MD (General Medicine)",
     [(Q, 200, 1), (S, 400, 1), (H, 100, 10)],
     [(Q, 1, "09:00", "12:00"), (Q, 2, "09:00", "12:00"), (S, 3, "14:00", "16:00"), (H, 6, "09:00", "12:00")]),
    ("Mohammed", "Khan", 9, 20, 15, "English, Urdu, Hindi, Telugu", "MBBS, MD (Pulmonary Medicine)",
     [(S, 500, 1), (D, 900, 1)],
     [(S, 1, "10:00", "13:00"), (S, 3, "10:00", "13:00"), (D, 5, "15:00", "17:00")]),
    ("Divya", "Menon", 1, 3, 12, "English, Malayalam", "MBBS, MD, DM (Endocrinology)",
     [(S, 900, 1), (D, 1500, 1)],
     [(S, 2, "14:00", "17:00"), (D, 4, "09:00", "11:00"), (S, 6, "10:00", "13:00")]),
    ("Suresh", "Kumar", 10, 4, 22, "English, Telugu", "MBBS, DNB (Family Medicine)",
     [(Q, 150, 1), (S, 300, 1)],
     [(Q, 1, "08:00", "11:00"), (Q, 3, "08:00", "11:00"), (Q, 5, "08:00", "11:00"), (S, 6, "14:00", "17:00")]),
    ("Meera", "Joshi", 2, 19, 16, "English, Hindi, Marathi", "MBBS, MD (Psychiatry)",
     [(D, 1500, 1), (S, 1000, 1)],
     [(D, 1, "14:00", "17:00"), (S, 4, "15:00", "18:00"), (D, 7, "10:00", "12:00")]),
    ("Aditya", "Gupta", 5, 11, 11, "English, Hindi", "MBBS, MD, DM (Nephrology)",
     [(S, 1000, 1), (D, 1700, 1)],
     [(S, 2, "09:00", "12:00"), (D, 5, "09:00", "11:00")]),
    ("Kavya", "Reddy", 4, 15, 8, "English, Telugu", "MBBS, MS (Ophthalmology)",
     [(Q, 300, 1), (S, 600, 1), (H, 200, 6)],
     [(Q, 1, "10:00", "12:00"), (S, 3, "10:00", "13:00"), (Q, 4, "15:00", "17:00"), (H, 6, "14:00", "16:00")]),
    ("Ravi", "Teja", 3, 17, 9, "English, Telugu, Tamil", "MBBS, MS (ENT)",
     [(Q, 300, 1), (S, 550, 1)],
     [(Q, 2, "10:00", "12:00"), (S, 4, "10:00", "13:00"), (Q, 6, "10:00", "12:00")]),
]


def account(n, first, last):
    """Login details for doctor number n (1-15). Passwords meet the sign-up rules (12+ chars, upper, lower, digit, symbol)."""
    return {"email": f"{first}.{last}@medislot.demo".lower(), "password": f"{first}@MediSlot{n:02d}",
            "license": f"TS-{7000 + n}"}


def call(api, method, path, token=None, body=None):
    h = {"content-type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(api.rstrip("/") + path, method=method, headers=h,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw or b"{}")
        except ValueError:
            return e.code, {"message": raw[:200].decode(errors="replace")}


def hhmm(v):
    return str(v)[:5]


def seed_one(api, n, doc):
    first, last, hospital, spec, years, langs, quals, types, patterns = doc
    acc = account(n, first, last)
    name = f"Dr. {first} {last}"
    st, body = call(api, "POST", "/auth/register/doctor", body={
        "email": acc["email"], "password": acc["password"], "firstName": first, "lastName": last,
        "hospitalId": hospital, "specializationId": spec, "licenseNumber": acc["license"], "yearsExperience": years,
        "clinicAddress": HOSPITALS[hospital], "qualifications": quals, "languages_spoken": langs})
    if st == 201:
        print(f"[{n:02d}] {name}: account created")
    elif st == 409:
        print(f"[{n:02d}] {name}: account already exists, signing in")
    else:
        print(f"[{n:02d}] {name}: registration failed HTTP {st}: {body.get('message')}")
        return False
    st, body = call(api, "POST", "/auth/login", body={"email": acc["email"], "password": acc["password"]})
    if st != 200 or not body.get("accessToken"):
        print(f"      sign-in failed HTTP {st}: {body.get('message')} (was this email registered with another password?)")
        return False
    token = body["accessToken"]

    st, body = call(api, "GET", "/doctors/me/consultation-types", token)
    have = {int(t["consultation_type_id"]) for t in (body.get("items") or [])} if st == 200 else set()
    for tid, fee, cap in types:
        if tid in have:
            continue
        st, body = call(api, "POST", "/doctors/me/consultation-types", token,
                        {"consultationTypeId": tid, "fee": fee, "capacity": cap})
        ok = st in (200, 201)
        print(f"      {'+' if ok else '!'} {TYPES[tid][0]} Rs {fee}" + ("" if ok else f"  HTTP {st}: {body.get('message')}"))

    st, body = call(api, "GET", "/doctors/me/availability-patterns", token)
    existing = {(int(p["consultation_type_id"]), int(p["day_of_week"]), hhmm(p["start_time"]), hhmm(p["end_time"]))
                for p in (body.get("items") or []) if p.get("is_active")} if st == 200 else set()
    for tid, dow, start, end in patterns:
        if (tid, dow, start, end) in existing:
            continue
        st, body = call(api, "POST", "/doctors/me/availability-patterns", token,
                        {"consultationTypeId": tid, "dayOfWeek": dow, "startTime": start, "endTime": end})
        ok = st in (200, 201)
        print(f"      {'+' if ok else '!'} {DAYS[dow]} {start}-{end} {TYPES[tid][0]}" + ("" if ok else f"  HTTP {st}: {body.get('message')}"))
    return True


def credentials_table():
    lines = ["| # | Doctor | Specialization | Hospital | Email | Password |", "|---|---|---|---|---|---|"]
    for n, (first, last, hospital, spec, *_rest) in enumerate(DOCTORS, start=1):
        acc = account(n, first, last)
        lines.append(f"| {n} | Dr. {first} {last} | {SPECIALIZATIONS[spec]} | {HOSPITALS[hospital]} | {acc['email']} | `{acc['password']}` |")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", help="API base URL including the stage, e.g. https://<id>.execute-api.<region>.amazonaws.com/dev")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and the credentials; call nothing")
    a = ap.parse_args()
    if a.dry_run or not a.api:
        for n, (first, last, hospital, spec, years, _l, _q, types, patterns) in enumerate(DOCTORS, start=1):
            print(f"[{n:02d}] Dr. {first} {last} - {SPECIALIZATIONS[spec]}, {HOSPITALS[hospital]}, {years} yrs")
            print("      types: " + "; ".join(f"{TYPES[t][0]} Rs {fee}" + (f" (up to {cap})" if cap > 1 else "") for t, fee, cap in types))
            print("      hours: " + "; ".join(f"{DAYS[d]} {s}-{e} {TYPES[t][0]}" for t, d, s, e in patterns))
        print("\n" + credentials_table())
        if not a.api and not a.dry_run:
            print("\nPass --api <url> to create these doctors.")
        return 0
    failures = sum(not seed_one(a.api, n, doc) for n, doc in enumerate(DOCTORS, start=1))
    print("\n" + credentials_table())
    print(f"\nDone: {len(DOCTORS) - failures} of {len(DOCTORS)} doctors ready. Slots appear within a minute (slot materialiser).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
