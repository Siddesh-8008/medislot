import bcrypt
def test_bcrypt_hash_is_not_plaintext_and_cost_is_12():
    password=b"StrongPass!234"
    hashed=bcrypt.hashpw(password,bcrypt.gensalt(rounds=12))
    assert hashed != password
    assert bcrypt.checkpw(password,hashed)
    assert int(hashed.split(b"$")[2]) >= 12
def test_password_policy_examples():
    import re
    def valid(p):
        return len(p)>=12 and len(p.encode())<=72 and re.search(r"[a-z]",p) and re.search(r"[A-Z]",p) and re.search(r"\d",p) and re.search(r"[^A-Za-z0-9]",p)
    assert valid("StrongPass!234")
    assert not valid("short1!")
    assert not valid("alllowercase123!")
    assert not valid("ALLUPPERCASE123!")


def test_doctor_license_format():
    import re
    pattern = re.compile(r"^[A-Z][A-Z ]{1,29}-\d{4}$")
    assert pattern.fullmatch("TS-1234")
    assert not pattern.fullmatch("TS-123")
    assert pattern.fullmatch("TELANGANA-1234")
    assert pattern.fullmatch("ANDHRA PRADESH-1234")
    assert not pattern.fullmatch("ts-1234")
