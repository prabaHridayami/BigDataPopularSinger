from instagram.client import InstagramAPI
access_token = "4164480119.50b101b.1ada2efb770045a4b3063cb658989723"

api = InstagramAPI(client_secret='e1d06876525045fc8b5c8b31afa056b2', access_token = access_token[0])
usr = api.user_search('praba_ba')

print (usr)
