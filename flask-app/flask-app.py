import os
import pathlib

import google.auth.transport.requests
import requests
from flask import Flask, abort, redirect, request, session
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol

app = Flask("Intelligent Email Archive")
app.secret_key = "intelligent-email-app"

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

GOOGLE_CLIENT_ID = "762564498507-gffp48vk20vsnbpjmoo47177j5avjt86.apps.googleusercontent.com"
client_secrets_file = os.path.join(pathlib.Path(__file__).parent, "client_secret.json")

#flow holds the information about how we want to authorise out users
flow = Flow.from_client_secrets_file(
    client_secrets_file=client_secrets_file,
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email", "openid", "https://mail.google.com/", "https://www.googleapis.com/auth/gmail.labels", "https://www.googleapis.com/auth/gmail.metadata", "https://www.googleapis.com/auth/gmail.modify", "https://www.googleapis.com/auth/gmail.readonly"],
    redirect_uri="http://127.0.0.1:5000/callback"    
            )

def login_is_required(function):
    def wrapper(*args, **kwargs):
        
        if "google_id" not in session:
            return abort(401) #Authorization required
        else:
            return function()

    return wrapper

#redirect user to google consent screen
@app.route("/login")
def login():
    #directing the user to the google consent screen using the flow functon we defined above
    authorization_url, state = flow.authorization_url()
    session["state"] = state #checking if state we initially created and the state returned are the same, to ensure no third party hooked on to our request.
    return redirect(authorization_url)

#receive data from google endpoint
@app.route("/callback")
def callback():
    flow.fetch_token(authorization_response=request.url)

    if not session["state"] == request.args["state"]:
        abort(500) #state does not match, hence abort (protecting app from cross-site attacks)

    credentials = flow.credentials
    request_session = requests.session()
    cached_session = cachecontrol.CacheControl(request_session)
    token_request = google.auth.transport.requests.Request(session=cached_session)

    id_info = id_token.verify_oauth2_token(
        id_token=credentials._id_token,
        request=token_request,
        audience=GOOGLE_CLIENT_ID
    )

    session["google_id"] = id_info.get("sub")
    session["name"] = id_info.get("name")
    return redirect("/protected_area")

#clear local session from our user
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

#index page
@app.route("/")
def index():
    return "Hello World <a href='/login'><button>Login</button></a>"

# this page only shown if user is logged in
@app.route("/protected_area")
@login_is_required
def protected_area():
    return "Protected! <a href='/logout'><button>Logout</button></a>"

if __name__ == "__main__":
    app.run(debug = True)
