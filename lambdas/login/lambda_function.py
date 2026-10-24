import os, time, uuid, bcrypt, jwt, secrets
from common import config, dbconn, body, response, emit, correlation, auth_table, token_hash
def handler(event, context):
    cid=correlation(event)
    try:
        data=body(event)
        if not isinstance(data,dict): return response(400,{"message":"Invalid JSON body","correlationId":cid})
        email=str(data.get("email","")).strip().lower(); password=data.get("password","")
        if not email or not isinstance(password,str): return response(400,{"message":"Email and password are required","correlationId":cid})
        c=config(); conn=dbconn(c)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id,password_hash,role,is_active FROM users WHERE email=%s",(email,))
                user=cur.fetchone()
        finally: conn.close()
        if not user or not user["is_active"] or not bcrypt.checkpw(password.encode(),user["password_hash"].encode()):
            emit("warning","login_rejected",cid)
            return response(401,{"message":"Invalid email or password","correlationId":cid})
        now=int(time.time()); jti=str(uuid.uuid4()); minutes=int(__import__("common").get_param(f"{c['prefix']}/auth/access-token-minutes"))
        claims={"sub":str(user["user_id"]),"userId":str(user["user_id"]),"role":user["role"],"iss":"medislot","aud":"medislot-api","iat":now,"exp":now+minutes*60,"jti":jti}
        access=jwt.encode(claims,c["jwt"],algorithm="HS256")
        raw=secrets.token_urlsafe(48); days=int(__import__("common").get_param(f"{c['prefix']}/auth/refresh-token-days"))
        auth_table().put_item(Item={"token_key":"REFRESH#"+token_hash(raw),"token_type":"REFRESH","user_id":str(user["user_id"]),"role":user["role"],"created_at":now,"expires_at":now+days*86400,"status":"ACTIVE"})
        emit("info","login_succeeded",cid,userId=str(user["user_id"]),role=user["role"])
        return response(200,{"accessToken":access,"tokenType":"Bearer","expiresIn":minutes*60,"refreshToken":raw,"userId":str(user["user_id"]),"role":user["role"]})
    except Exception:
        emit("error","login_failed",cid)
        return response(500,{"message":"Internal server error","correlationId":cid})
