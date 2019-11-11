import sys
import socket
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

class TweetListener(StreamListener):
    def __init__(self, socket):
        print("Tweet Listerner initializing..")
        self.client_socket = socket

    def  on_data(self, data):
        try:
            jsonMessage = json.loads(data)
            message = jsonMessage["text"].encode("utf-8")
            print(message)
            self.client_socket.send(message)
        except BaseException as e:
            print("Error on_data : %s" %str(e))
        return True
    
    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        print(status_code)
        return True

def connect_to_twitter(connection, tracks):
    api_key = "xxxx"
    api_secret_key = "xxxxxx"
    access_token = "xxxxx"
    access_token_secret = "xxxxxxxxx"

    auth = OAuthHandler(api_key,api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetListener(connection))
    print(tracks)
    twitter_stream.filter(track=tracks, languages=["en"])


if __name__ == "__main__":
    if  len(sys.argv) <  4:
        print("Usage : pythpn twtterStreaming.py <host> <port>  <tracks>", file=sys.stderr)
        exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    tracks = sys.argv[3]

    s = socket.socket()
    s.bind((host, port))

    tracks = tracks.split(" ")
    print("Tracks {}".format(str(tracks)))

    print("Listing to port {}".format(str(port)))   
    s.listen(5)

    connection, client_address = s.accept()

    print("Receieved request from: ",str(client_address))
    print("Initializing Listerner  to this trach :", tracks)

    connect_to_twitter(connection, tracks)
