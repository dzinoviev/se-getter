import time
import pickle
import sys
import csv
from itertools import chain, combinations, groupby
from collections import Counter
from urllib.request import urlopen
from urllib.error import HTTPError

from bs4 import BeautifulSoup as bs
import networkx as nx

# Modify the site name, as needed
SITE = "stackoverflow.com"

def catch_and_cache(downloader, pickle_filename, depth=10000):
    # Has the data block been cached?
    try:
        # Avoid stack overflows
        sys.setrecursionlimit(depth)
        with open(pickle_filename, "rb") as infile:
            data = pickle.load(infile)
    # The block was not cached
    except FileNotFoundError:
        # Fetch it, cache it
        data = downloader()
        with open(pickle_filename, "wb") as outfile:
            pickle.dump(data, outfile)
    return data

def get_popular_tags():
    BASE = "https://{}/tags?page={}&tab=popular"
    TAG_PAGES = 10
    popular_tags = {}
    for i in range(1, 1 + TAG_PAGES):
        try:
            html = urlopen(BASE.format(SITE, i)).read()
        except HTTPError:
            continue
        soup = bs(html, features="lxml")
        taglinks = soup.findAll("a", rel="tag")
        tags = {
            a.string:
            int(a.nextSibling.find("span",
                                   class_="item-multiplier-count").string)
            for a in taglinks if a.nextSibling != "\n"}
            # Because some tags have not been used^^^
        popular_tags.update(tags)
        time.sleep(1) # Be nice!
    return popular_tags

def get_question_tags():
    BASE = "https://{}/questions/tagged/{}?sort={}&page={}&pagesize=50"
    TAG_PAGES = 10
    question_tags = []
    seen = set() # All processed questions
    for i, tag in enumerate(popular_tags, 1):
        # Progress reporting
        print("{} ({} of {})".format(tag, i, len(popular_tags)))
        for how in "newest", "frequent":
            for page in range(1, 1 + TAG_PAGES):
                try:
                    time.sleep(1)
                    html = urlopen(BASE.format(SITE, tag, how, page)).read()
                except HTTPError:
                    continue
                soup = bs(html, features="lxml")
                questions = soup.findAll("div", class_="question-summary")
                taglists = [(qn["id"],
                             [tag.text for tag
                              in qn.find_all("a", class_="post-tag")])
                            for qn in questions]
                # Eliminate duplicate questions
                unseen = {qid for qid, _ in taglists} - seen
                new_questions = [tags for qid, tags in taglists
                                 if qid in unseen]
                question_tags.extend(new_questions)
                seen |= unseen
    return question_tags

# Download the data
try:
    with open("TAGS") as tagfile:
        print("Reading popular tags")
        popular_tags = tagfile.read().split()
except:
    print("Downloading the popular tags")
    popular_tags = catch_and_cache(get_popular_tags, "tags-{}.p".format(SITE))
print("Downloading the questions")
question_tags = catch_and_cache(get_question_tags, "data-{}.p".format(SITE))

# Build the network
EDGE_SLICING_THRESHOLD = 6
NODE_SLICING_THRESHOLD = 8
NODE_LIMIT = 1000

edges = chain.from_iterable(((w1, w2) if w1 < w2 else (w2, w1)
                             for w1, w2 in combinations(t, 2))
                            for t in question_tags)

wedges = Counter(edges)
max_weight = max(wedges.values())
edgelist = [(n1, n2, {"weight": w / max_weight})
            for ((n1, n2), w) in wedges.items()
            if w >= EDGE_SLICING_THRESHOLD] # Slicing
G = nx.Graph(edgelist)
if len(G) > NODE_LIMIT:
    G = nx.subgraph(G, [k for k, v in dict(nx.degree(G)).items()
                        if v >= NODE_SLICING_THRESHOLD])

nx.set_node_attributes(G, popular_tags, "s")
nx.set_node_attributes(G, nx.eigenvector_centrality(G), "eig")

# Is community detections possible?
try:
    import community
except ModuleNotFoundError:
    print("Run pip install python-louvain' to enable community detection")
    nx.write_graphml(G, "map-{}.graphml".format(SITE))
    sys.exit()

parts = community.best_partition(G)
nx.set_node_attributes(G, parts, "part")
print("Modularity: {}".format(community.modularity(parts, G)))

TOP_HOWMANY = 5
top = [[k for k, w in sorted(v, key=lambda n: n[1].get("s", 0),
                             reverse=True)[:TOP_HOWMANY]]
       for k, v in groupby(sorted(dict(G.nodes(data=True)).items(),
                                  key=lambda n: n[1]["part"]),
                           key=lambda n: n[1]["part"])]
with open("top-{}.csv".format(SITE), "w") as csvout:
    writer = csv.writer(csvout)
    writer.writerows(top)

# Induced graph
I = community.induced_graph(parts, G)
cluster_sizes = {k: sum(s for k, s in v)
                 for k, v
                 in groupby(sorted([(y["part"], y["s"])
                                    for x, y
                                    in dict(G.nodes(data=True)).items()
                                    if "s" in y]),
                            key=lambda x: x[0])}
nx.set_node_attributes(I, cluster_sizes, "s")
TOP_LABELS = 2
labels = {i: ", ".join(x[0:TOP_LABELS]) for i, x in enumerate(top)}
I = nx.relabel_nodes(I, labels)
I.remove_edges_from(list(nx.selfloop_edges(I)))

# Save the results
nx.write_graphml(G, "map-{}.graphml".format(SITE))
nx.write_graphml(I, "induced-{}.graphml".format(SITE))
