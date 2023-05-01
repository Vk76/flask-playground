import json
import psycopg2
from hashring import HashRing
import requests



def lot_of_writes():
    url = "http://127.0.0.1:81/"

    headers = {
        'Content-Type': 'application/json'
    }

    for i in range(1000,100000):
        payload = json.dumps({
            "url": "https://url"+str(i)+".com"
        })
        response = requests.request("POST", url, headers=headers, data=payload)


# lot_of_writes()



def test_failure_after_adding_node():
    hashRing = HashRing(["5432", "5433", "5434", "5435"])
    conn = psycopg2.connect(database="my_database", user="postgres",
                            password="postgres", host="localhost", port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT url_id From url_table")
    urlIds = cursor.fetchall()
    count = 0
    for id in urlIds:
        node = hashRing.get_node(id[0])
        if node != "5432":
            print(id,node)
            count += 1
    cursor.close()
    conn.close()
    print("Number of ids in shard1 mapped to other shards", count) 
    # Number of ids in shard1 mapped to other shards 9054


test_failure_after_adding_node()


