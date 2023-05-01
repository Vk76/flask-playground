from flask import Flask, request, jsonify, json
import psycopg2
import hashlib
from hashring import HashRing

app = Flask(__name__)

DATABASE = "my_database"
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"


SERVERS = {
    "5432" : psycopg2.connect(database=DATABASE, user=USER,
                     password=PASSWORD, host=HOST, port="5432"),
    "5433":psycopg2.connect(database=DATABASE, user=USER,
                     password=PASSWORD, host=HOST, port="5433"),
    "5434": psycopg2.connect(database=DATABASE, user=USER,
                     password=PASSWORD, host=HOST, port="5434"),
    "5435": psycopg2.connect(database=DATABASE, user=USER,
                             password=PASSWORD, host=HOST, port="5435")
}

hashRing = HashRing(["5432","5433","5434","5435"])



@app.route('/',methods=["GET"])
def getUrl():
    urlId = request.args.get("urlId")
    server = hashRing.get_node(urlId)
    connection = SERVERS[server]
    cur = connection.cursor()
    print(type(urlId))
    cur.execute(
        "SELECT URL FROM URL_TABLE WHERE URL_ID = %s",(urlId,))
    queryResponse = cur.fetchall()
    cur.close()
    connection.commit()
    response = { "urls" : queryResponse }
    return jsonify(response)


@app.route('/', methods=["POST"])
def addUrl():
    url = json.loads(request.data)['url']
    urlId = hashlib.sha256(url.encode('utf-8')).hexdigest()[:5]
    server = hashRing.get_node(urlId)
    connection = SERVERS[server]
    cur = connection.cursor()
    cur.execute(
        "INSERT INTO URL_TABLE (URL_ID,URL) VALUES (%s,%s)", (urlId, url))

    cur.close()
    connection.commit()

    response = {
        "urlId": urlId,
        "url": url,
        "server": server
    }

    return jsonify(response)


app.run(host='0.0.0.0', port=81)
