import collections
import json
import smtplib
import string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import spacy
from bson import json_util
from flask import Flask
from flask import request
from nltk.corpus import stopwords
from pymongo import MongoClient
from sklearn.cluster import KMeans

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
    # tokens = [t for t in tokens if t not in stopwords.words('english')]
    # nlp = spacy.load('it')
    # doc = nlp(str(question))
    doc = nlp(str(tokens))
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
    msg["Subject"] = "New peak search of Cronache Maceratesi"
    msg["From"] = sender_email
    msg["To"] = "paoletti99.bb@gmail.com"
    text = "Test"
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
def hello_world():  # put application's code here
    return 'Hello World!'


# Svuotare il db ogni giorno


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
    return "True"


@app.route('/addQuery', methods=['POST'])
def add_query():
    if collection.insert_one(json.loads(json_util.dumps(request.get_json()))) and verify_query(request.get_json()):

        # Se viene aggiunta una query correttamente, viene ricalcolato l'algoritmo k-means su tutte le query presenti
        # nel db.

        questions = list(collection.find({}))
        if len(questions) >= nclusters:
            clusters = cluster_questions(questions, nclusters)
            # question_clusters = []

            for i, cluster in enumerate(clusters):
                question_cluster = []
                for j, sentence in enumerate(clusters[cluster]):
                    question_cluster.append(questions[sentence])

                # Se esiste un cluster con un numero strano (da definire, idea: variabile globale) di elementi,
                # viene attivata la funzione per inviare la notifica al sistema
                # Tutte le Query che sono state coinvolte per l'attivazione del trigger devono essere eliminate,
                # sennò alla n+1 query rifaranno ripartire la notifica
                if len(question_cluster) >= limit:
                    sent_notification(question_cluster)
                    clear_db(question_cluster)
                # question_clusters.append(question_cluster)

        return request.get_json()
    else:
        return "query not added"


if __name__ == '__main__':
    app.run()
