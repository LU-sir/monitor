import json
import requests
import pymysql
import time
import os


db = pymysql.connect(host=os.getenv("DB_HOST"),
                     user=os.getenv("DB_USER"),
                     password=os.getenv("DB_PASSWD"),
                     database=os.getenv("DB_USER"))

cursor = db.cursor()
networkID = os.getenv("ZT_NETWORKID")
authKey = os.getenv("ZT_APIKEY")

currentTime = int(time.time())


def getMemberInfo(networkID: str, authKey: str) -> list:
    url = "https://my.zerotier.com/api/network/{0}/member".format(networkID)
    headers = {
        "Authorization": "bearer {0}".format(authKey)
    }
    r = requests.get(url, headers=headers)
    if r:
        return json.loads(r.text)


def isNewMember(memberInfo: dict) -> bool:
    selectSQL = "SELECT id FROM machine WHERE id='{}'".format(memberInfo["nodeId"])
    cursor.execute(selectSQL)
    results = cursor.fetchall()
    if results:
        return False
    return True


def addMember(memberInfo: dict):
    insertSQL = "INSERT INTO machine(id, name, description, ip) VALUES ('{}', '{}', '{}', '{}')".format(memberInfo["nodeId"], memberInfo['name'], memberInfo['description'], memberInfo["config"]["ipAssignments"][0])
    try:
        # 执行sql语句
        cursor.execute(insertSQL)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()
    addTableSQL = "CREATE TABLE m{}(cur INT , realIp CHAR(15))".format(memberInfo["nodeId"])
    cursor.execute(addTableSQL)
    sql = "INSERT INTO m{0} (cur, realIp) VALUES ({1}, '')".format(memberInfo["nodeId"], currentTime)
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()


def addMemberStas(memberInfo: dict):
    selectRecentSQL = "SELECT * FROM m{0} WHERE cur=(SELECT max(cur) FROM m{0})".format(memberInfo["nodeId"])
    cursor.execute(selectRecentSQL)
    reselts = cursor.fetchall()
    sql = ""
    if reselts[0][1] == "":  # it was off
        if not memberInfo["online"]:  # it is off
            sql = "UPDATE m{0} SET cur={1} WHERE cur={2}".format(memberInfo["nodeId"], currentTime, reselts[0][0])
        else:  # it is on
            sql = "INSERT INTO m{0} (cur, realIp) VALUES ({1}, '{2}')".format(memberInfo["nodeId"], currentTime,
                                                                              memberInfo["physicalAddress"])
    else:  # it was on
        if not memberInfo["online"]:  # it is off
            sql = "INSERT INTO m{0} (cur) VALUES ({1})".format(memberInfo["nodeId"], currentTime)
        else:  # it is on
            if reselts[0][1] == "{}".format(memberInfo["physicalAddress"]):
                sql = "UPDATE m{0} SET cur={1} WHERE cur={2}".format(memberInfo["nodeId"], currentTime, reselts[0][0])
            else:
                sql = "INSERT INTO m{0} (cur, realIp) VALUES ({1}, '{2}')".format(memberInfo["nodeId"], currentTime,
                                                                                  memberInfo["physicalAddress"])
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()


if __name__ == "__main__":
    memInfo = getMemberInfo(networkID=networkID, authKey=authKey)

    for mem in memInfo:
        if isNewMember(mem):
            addMember(mem)
        else:
            addMemberStas(mem)

    # print(memInfo)
    db.close()
