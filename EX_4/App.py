# ----מגישים: Amitay Mantzur, Jonhatan Aaron, Alon Geller

# python3 -m flask run --host=127.0.0.1
# python3 -m flask run --host=127.0.0.2
# python3 -m flask run --host=127.0.0.3

# ---- Imports ---- #
import sys
import json
import requests

from flask_apscheduler import APScheduler
from flask import Flask, request, jsonify
from bloom_filter2 import BloomFilter

# ---- End of imports ---- #
# ---- Global variables ---- #
app = Flask(__name__)  # App name
res = ""  # Variable to save the response code received from request
scheduler = APScheduler()  # Scheduler object
current = sys.argv[2].rsplit('=', 1)[1]  # current host ip
Votes_total = {}  # Votes from all the locations
Users_total = {}  # Users from all the locations
VOTES = {}  # Votes from current location
USERS = {}  # Users from current location
USERS_1 = {}  # Save Server 1 users
USERS_2 = {}  # Save Server 2 users
USERS_3 = {}  # Save Server 3 users
leader = 1  # Set a leader server that would be in charge
USERS_BLOOM_1 = BloomFilter(max_elements=1000000,
                            error_rate=0.01, filename="voters.bloom" + current + "_1", start_fresh=True)
# Bloom Filter for server 1
USERS_BLOOM_2 = BloomFilter(max_elements=1000000,
                            error_rate=0.01, filename="voters.bloom" + current + "_2", start_fresh=True)
# Bloom Filter for server 2
USERS_BLOOM_3 = BloomFilter(max_elements=1000000,
                            error_rate=0.01, filename="voters.bloom" + current + "_3", start_fresh=True)


# Bloom Filter for server 3


# ---- This function is the scheduled function, the function updates the other servers with the information ---- #

def updates():
    # Creating my JSON object for post request
    data = {
        "Users": json.dumps(Users_total),
        "votes": json.dumps(Votes_total),
        "1": json.dumps(USERS_1),
        "2": json.dumps(USERS_2),
        "3": json.dumps(USERS_3),
    }
    # If the leader is 1 than if its 127.0.0.1 he would send the info
    if (leader == 1):
        if (current == "127.0.0.1"):
            # Try to send the info to 127.0.0.2 and 127.0.0.3
            try:
                requests.post("http://127.0.0.2:5000/total", data=data)
            except:
                pass
            try:
                requests.post("http://127.0.0.3:5000/total", data=data)
            except:
                pass
    # If the leader is 2 than if its 127.0.0.2 he would send the info
    elif (leader == 2):
        if (current == "127.0.0.2"):
            # Try to send the info to 127.0.0.1 and 127.0.0.3
            try:
                requests.post("http://127.0.0.1:5000/total", data=data)
            except:
                pass
            try:
                requests.post("http://127.0.0.3:5000/total", data=data)
            except:
                pass
    # If the leader is 3 than if its 127.0.0.3 he would send the info
    elif (leader == 3):
        if (current == "127.0.0.3"):
            # Try to send the info to 127.0.0.2 and 127.0.0.1
            try:
                requests.post("http://127.0.0.1:5000/total", data=data)
            except:
                pass
            try:
                requests.post("http://127.0.0.2:5000/total", data=data)
            except:
                pass


# ---- Enf of global variables ---- #

# ---- Flask application ---- #
# ---- this page is for collecting the votes, the user,vote and color chosen are passed through the url string ---- #

@app.route("/vote", methods=["GET", "POST"])
# This function controls the behavior of the page, the variables are collected from the url and the processed ---- #
def vote():
    global leader
    user = request.args.get('user')  # Get the user
    vote = request.args.get('vote')  # Get the vote
    color = request.args.get('color')  # Get the color he chose
    # Check if this user already voted
    if user in USERS_BLOOM_1 and user in USERS_1:  # Check BLOOM_FILTER for 127.0.0.1 and its users list
        return ("You already voted!", 429)
    if user in USERS_BLOOM_2 and user in USERS_2:  # Check BLOOM_FILTER for 127.0.0.2 and its users list
        return ("You already voted!", 429)
    if user in USERS_BLOOM_3 and user in USERS_3:  # Check BLOOM_FILTER for 127.0.0.3 and its users list
        return ("You already voted!", 429)
    # If so return a message that he already did
    USERS[user] = color  # Update current local USERS dic
    VOTES[vote] = VOTES.get(vote, 0) + 1  # Update current local VOTES dic
    Users_total[user] = color  # Update current total USERS dic
    Votes_total[vote] = Votes_total.get(vote, 0) + 1  # Update current total VOTES dic
    # Update the server info, add the user to the right BLOOM FILTER and the right users list
    if (current == "127.0.0.1"):
        USERS_BLOOM_1.add(user)
        USERS_1[user] = color
    elif (current == "127.0.0.2"):
        USERS_BLOOM_2.add(user)
        USERS_2[user] = color
    elif (current == "127.0.0.3"):
        USERS_BLOOM_3.add(user)
        USERS_3[user] = color
    # Prepare data to be sent to the other servers in order to inform them about the vote
    data = {
        "user": user,  # User voted
        "vote": vote,  # The vote he chose
        "color": color,  # The color he chose
        "sender": current
    }
    if (current != "127.0.0.1" and leader == 1):
        url = "http://127.0.0.1:5000/voted"
    # If the host is not the leader of the cluster(127.0.0.1) inform him about the vote
    try:
        requests.post(url, data)
    except:
        if current == "127.0.0.2":
            leader = 2
        if current == "127.0.0.3":
            leader = 3
        # If it its 127.0.0.1 inform the other stations
        # If response is 400 than 127.0.0.1 cannot be found so 127.0.0.2 or 127.0.0.3 would be the leader

    return ("Thank you for voting", 200)


# ---- This page is for showing the current station status of voters ---- #
@app.route("/status", methods=["GET"])
# This function shows the current status of votes in the station
def status():
    colors = {}
    for color in USERS.values():
        colors[color] = colors.get(color, 0) + 1
    return ({"Votes": VOTES, "Colors": colors}, 200)


# ---- This page is for handling messages between servers ---- #
@app.route("/voted", methods=["POST"])
# This app receives a message from a server and processes it
def voted():
    user_2 = request.form["user"]  # Get user
    vote_2 = request.form["vote"]  # Get vote
    color_2 = request.form["color"]  # Get color
    sent_from = request.form['sender']
    Users_total[user_2] = color_2  # Insert to total users
    Votes_total[vote_2] = Votes_total.get(vote_2, 0) + 1  # Insert to total votes
    # Update the server info, add the user to the right BLOOM FILTER and the right users list
    if (sent_from == "127.0.0.1"):
        USERS_BLOOM_1.add(user_2)
    elif (sent_from == "127.0.0.2"):
        USERS_BLOOM_2.add(user_2)
    elif (sent_from == "127.0.0.3"):
        USERS_BLOOM_3.add(user_2)
    return ("Thank you for voting", 200)


# ---- This page is for showing the current total status of voters ---- #
@app.route("/all", methods=["GET"])
# This function shows the total votes status
def all():
    total_colors = {}
    for color in Users_total.values():
        total_colors[color] = total_colors.get(color, 0) + 1
    return ({"Votes": Votes_total, "Colors": total_colors, "Num of votes": len(Users_total)}, 200)


@app.route("/total", methods=["GET", "POST"])
# This function shows the total votes status
def total():
    # ---- Loading the data from the request ---- #
    Users = json.loads(request.form["Users"])
    votes = json.loads(request.form["votes"])
    User_1 = json.loads(request.form["1"])
    User_2 = json.loads(request.form["2"])
    User_3 = json.loads(request.form["3"])
    # ---- Data loaded ---- #
    # ---- Check each BLOOM FILTER and users list to see if update is needed ---- #
    for users in User_1.keys():
        if (users not in USERS_BLOOM_1):
            USERS_BLOOM_1.add(users)
        if (users not in USERS_1):
            USERS_1[users] = User_1[users]
        if (users not in Users_total):
            Users_total[users] = Users[users]
    for users in User_2.keys():
        if (users not in USERS_BLOOM_2):
            USERS_BLOOM_2.add(users)
        if (users not in USERS_2):
            USERS_2[users] = User_2[users]
        if (users not in Users_total):
            Users_total[users] = Users[users]
    for users in User_3.keys():
        if (users not in USERS_BLOOM_3):
            USERS_BLOOM_3.add(users)
        if (users not in USERS_3):
            USERS_3[users] = User_3[users]
        if (users not in Users_total):
            Users_total[users] = Users[users]
    # ---- Ended checking all the lists and BLOOM FILTERS
    # ---- Check the votes to see if there are new ones ---- #
    for v in votes.keys():
        Votes_total[v] = votes[v]
    return ("Thank you for voting", 200)


# ---- End of flask pages ---- #

# ---- Scheduler configuration and activation ---- #
app.config['JOBS'] = [
    {
        'id': 'send_post_request_job',
        'func': updates,
        'args': (),
        'trigger': 'interval',
        'seconds': 10
    }
]

scheduler.init_app(app)
scheduler.start()
# ----End of scheduler configuration and activation ---- #


# ---- Run flask app ---- #
if __name__ == '__main__':
    app.run(debug=True)

# ---- End of running the flask app ---- #


# ---- End of file ---- #
