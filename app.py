import collections
import json
import string

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


def clean_string(inp_str):
    st = str.maketrans('', '', string.punctuation)
    return inp_str.translate(st).lower().strip().split()


def convert_to_vec(question):
    tokens = clean_string(question)
    tokens = [t for t in tokens if t not in stopwords.words('italian')]
    # nlp = spacy.load('it')
    doc = nlp(str(question))
    return doc.vector


def cluster_questions(questions, nb_of_clusters=5):
    question_vectors = [convert_to_vec(question["text"]) for question in questions]
    kmeans = KMeans(n_clusters=nb_of_clusters)
    kmeans.fit(question_vectors)
    clusters = collections.defaultdict(list)
    for i, label in enumerate(kmeans.labels_):
        clusters[label].append(i)
    return dict(clusters)


def sent_notification(questions):
    return True


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


# Svuotare il db ogni giorno
# Tutte le Query che sono state coinvolte per l'attivazione del trigger devono essere eliminate,
# sennò alla n+1 query rifaranno ripartire la notifica


@app.route('/addQuery', methods=['POST'])
def add_query():
    req = request.get_json()
    if collection.insert_one(json.loads(json_util.dumps(request.get_json()))):

        # Se viene aggiunta una query correttamente, viene ricalcolato l'algoritmo k-means su tutte le query presenti
        # nel db.

        questions = collection.find({})
        nclusters = 22
        clusters = cluster_questions(questions, nclusters)
        question_clusters = []
        for cluster in range(nclusters):
            question_cluster = []
            for i, sentence in enumerate(clusters[cluster]):
                question_cluster.append([str(questions[sentence])])
            question_clusters.append(question_cluster)

        # Se esiste un cluster con un numero strano (da definire, idea: variabile globale) di elementi,
        # viene attivata la funzione per inviare la notifica al sistema

        for question_cluster in question_clusters:
            if len(question_cluster) >= 10:
                sent_notification(question_cluster)

        return req
    else:
        return "query not added"


if __name__ == '__main__':
    app.run()
