import collections
import json
import math
import smtplib
import string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import matplotlib.pyplot as plt
import spacy
from bson import json_util
from flask import Flask
from flask import request
from nltk.corpus import stopwords
from pymongo import MongoClient
from sklearn.cluster import KMeans

from threading import Thread
from datetime import datetime
import requests
import time


# QUERY ANALYSIS


app = Flask(__name__)

nlp = spacy.load('it_core_news_sm')

client = MongoClient()
db = client['Query']
collection = db['query']

limit = 10
nclusters = 25

server = None


def clean_string(inp_str):
    st = str.maketrans('', '', string.punctuation)
    return inp_str.translate(st).lower().strip().split()


def convert_to_vec(question):
    tokens = clean_string(question)
    tokens = [t for t in tokens if t not in stopwords.words('italian')]
    doc = nlp(str(question))
    return doc.vector


def cluster_questions(questions, nb_of_clusters=5):
    question_vectors = [convert_to_vec(question["text"]) for question in questions]
    kmeans = KMeans(n_clusters=nb_of_clusters).fit(question_vectors)
    clusters = collections.defaultdict(list)
    for i, label in enumerate(kmeans.labels_):
        clusters[label].append(i)
    return dict(clusters)


def sent_notification(questions):
    sender_email = "peak.search.notifier@gmail.com"
    tolist = ["humboorw@hi2.in", "paoletti99.bb@gmail.com"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "New peak of search on Cronache Maceratesi"
    msg["From"] = sender_email
    msg["To"] = "paoletti99.bb@gmail.com"
    text = ""
    for t in questions:
        text += t["text"] + "\n"
    msg.attach(MIMEText(text, "plain"))
    server.sendmail(sender_email, tolist, msg.as_string())

    return True


def clear_db(questions):
    for query in questions:
        collection.delete_one({'_id': query["_id"]})
    return True


def verify_query(query):
    if len(query["text"]) != 0:
        return True


@app.route('/')
def hello_world():  # check if everything is working
    return 'Hello World!'


# Svuotare il db ogni giorno
@app.route('/clear', methods=['POST'])
def clear():
    collection.delete_many({})
    return 'True'


@app.route('/verifyQuery', methods=['GET'])
def ver_query():
    if verify_query(request.get_json()):
        return "True"
    else:
        return "False"


@app.route('/setNCluster/<int:n_cluster>', methods=['POST'])
def set_n_cluster(n_cluster):
    if isinstance(n_cluster, int):
        global nclusters
        nclusters = n_cluster
        return "N cluster set to " + str(nclusters)
    return "N cluster not set"


@app.route('/setLimit/<int:n_limit>', methods=['POST'])
def set_limit(n_limit):
    if isinstance(n_limit, int):
        global limit
        limit = n_limit
        return "Limit set to " + str(limit)
    return "Limit not set"


@app.route('/login', methods=['POST'])
def login():
    try:
        global server
        server = smtplib.SMTP_SSL("smtp.gmail.com")
        server.login("peak.search.notifier@gmail.com", request.get_json()["pas"])
    except Exception as e:
        return str(e)
    return "Login in"


def cluster_variance(n):
    variances = []
    kmeans = []
    outputs = []
    K = [i for i in range(1, n + 1)]

    questions = list(collection.find({}))
    question_vectors = [convert_to_vec(question["text"]) for question in questions]

    for i in range(1, n + 1):
        variance = 0
        model = KMeans(n_clusters=i, random_state=82, verbose=2).fit(question_vectors)
        kmeans.append(model)
        variances.append(model.inertia_)

    return variances, K, n


@app.route('/draw', methods=['GET'])
def draw():
    variances, K, n = cluster_variance(nclusters)

    plt.plot(K, variances)
    plt.ylabel("Variance")
    plt.xlabel("K Value")
    # plt.xticks([i for i in range(1, n + 1)])
    plt.show()

    return "True"


@app.route('/addQuery', methods=['POST'])
def add_query():
    if server is None:
        return "Login not logged"
    else:
        if collection.insert_one(json.loads(json_util.dumps(request.get_json()))) and verify_query(request.get_json()):

            questions = list(collection.find({}))
            # if len(questions) >= nclusters:
            if len(questions) >= math.ceil(collection.count_documents({}) / 2):
                # clusters = cluster_questions(questions, nclusters)
                clusters = cluster_questions(questions, math.ceil(collection.count_documents({}) / 2))

                for i, cluster in enumerate(clusters):
                    question_cluster = []
                    for j, sentence in enumerate(clusters[cluster]):
                        question_cluster.append(questions[sentence])

                    if len(question_cluster) >= limit:
                        sent_notification(question_cluster)
                        clear_db(question_cluster)

            return request.get_json()
        else:
            return "Query not added"


def run_app():
    print("Query analyzer started.")
    app.run()


# FETCH QUERIES


def send_queries(queries):
    for q in queries:
        requests.post("http://127.0.0.1:5000/addQuery", data={'text': q})


def is_valid(string):
    return all(x.isalpha() or x.isspace() for x in string) and string != "ep_autosuggest_placeholder"


def too_close(current, previous):
    return (current - previous) < 100


def get_time_in_millis(single_line):
    query_time = single_line.split(']')[0][1:]
    dt_obj = datetime.strptime(query_time, '%Y-%m-%dT%H:%M:%S,%f')
    millis = dt_obj.timestamp() * 1000
    return millis


def get_query(complete_line):
    json_part = json.loads(complete_line[complete_line.find("source") + 7:-9])  # trim json part
    res = json_part["query"]["function_score"]["query"]["bool"]["should"][0]["bool"]["must"][0]["bool"]["should"][0]["multi_match"]["query"]  # retrieve actual query content
    return res

def query_fetcher():
    print("Query fetcher started.")
    while True:
        with open('/var/log/elasticsearch/csdproject_index_search_slowlog.log', 'r+', encoding="utf-8") as file:
            file_contents = file.read()
            query_list = []
            prev_query = ""
            prev_time = 0.0
            for line in file_contents.split('\n'):
                try:
                    query = get_query(line)
                    if is_valid(query) and (query != prev_query or not too_close(get_time_in_millis(line), prev_time)):
                        query_list.append(query)
                    prev_query = query
                    prev_time = get_time_in_millis(line)
                except Exception:
                    pass
            if len(query_list) > 0:
                send_queries(query_list)
            # print(len(query_list))
            # print(query_list)
            file.truncate(0)
            file.close()
        time.sleep(54000)  # Sleep 30 minutes = 54000


# EMPTY QUERY DATABASE


def empty_db():
    print("Empty DB started.")
    while True:
        time.sleep(172800)  # 2 days in seconds
        requests.post("http://127.0.0.1:5000/clear")


# MAIN


if __name__ == '__main__':
    try:
        t1 = Thread(target=run_app).start()
        t2 = Thread(target=query_fetcher).start()
        t3 = Thread(target=empty_db).start()
    except Exception as e:
        print("Unexpected error: " + str(e))
