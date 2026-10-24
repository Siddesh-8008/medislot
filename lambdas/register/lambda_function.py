import os, re, bcrypt
from common import config, dbconn, body, response, emit, correlation
def handler(event, context):
    cid=correlation(event)
    try:
        role=os.environ["ROLE"]
        data=body(event)
        if not isinstance(data,dict): return response(400,{"message":"Invalid JSON body","correlationId":cid})
        email=str(data.get("email","")).strip().lower()
        password=data.get("password","")
        full_name=str(data.get("fullName","")).strip()
        min_len=int(config()["env"] and __import__("common").get_param(f"/medislot/{os.environ.get('ENVIRONMENT','dev')}/auth/password-min-length"))
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+",email) or len(email)>254 or not full_name or len(full_name)>150:
            return response(400,{"message":"Valid email and fullName are required","correlationId":cid})
        if not isinstance(password,str) or len(password)<min_len or len(password.encode())>72 or not re.search(r"[a-z]",password) or not re.search(r"[A-Z]",password) or not re.search(r"\d",password) or not re.search(r"[^A-Za-z0-9]",password):
            return response(400,{"message":f"Password must be {min_len}-128 characters and include upper, lower, number and symbol","correlationId":cid})
        c=config()
        hashed=bcrypt.hashpw(password.encode(),bcrypt.gensalt(rounds=12)).decode()
        conn=dbconn(c)
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(email,password_hash,role,full_name) VALUES(%s,%s,%s,%s)",(email,hashed,role,full_name))
                uid=cur.lastrowid
            conn.commit()
        except Exception as e:
            conn.rollback()
            if "Duplicate entry" in str(e): return response(409,{"message":"Unable to register with those details","correlationId":cid})
            raise
        finally: conn.close()
        emit("info","registration_succeeded",cid,role=role,userId=str(uid))
        return response(201,{"userId":str(uid),"role":role,"message":"Registration successful"})
    except Exception:
        emit("error","registration_failed",cid)
        return response(500,{"message":"Internal server error","correlationId":cid})
